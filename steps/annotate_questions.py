import asyncio
import json
import re
import traceback
from collections import defaultdict
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple


async def annotate_questions_task(
        model_input: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: annotate questions

    [Params]
    model_input    : List[Dict[str, Any]]
    classification : Literal['high_header_sim', 'low_header_sim']
    model_name     : str
    semaphore      : asyncio.Semaphore

    [Return]
    model_output_list : List[Dict[str, Any]]
    cost              : int
    """
    ###
    if classification == 'high_header_sim':
        tasks = [
            get_async_openai_response(
                semaphore=semaphore,
                system_prompt=load_prompt(role='system', task='annotate_questions_with_high_header_sim'),
                user_prompt=load_prompt(role='user', task='annotate_questions_with_high_header_sim').format(
                    shots=input_data['shots'],
                    table_title_set="\n".join([
                        table_serialization(
                            table_num=tdx + 1,
                            metadata=table['metadata'],
                            header=None,
                            cell=None
                        )
                        for tdx, table in enumerate(input_data['gold_table_set']) 
                    ]),
                    overlapping_headers="Table headers are " + ", ".join(input_data['overlapping_headers']) + ".",
                    overlapping_cells="Overlapping cells are " + ", ".join([
                        f"{cell['value']} about {cell['col']}"
                        for cell in input_data['overlapping_cells']
                    ]) + "."
                ),
                model_name=model_name,
                key=input_data['key']
            )
            for input_data in model_input
        ]
    
    elif classification == 'low_header_sim':
        tasks = [
            get_async_openai_response(
                semaphore=semaphore,
                system_prompt=load_prompt(role='system', task='annotate_questions_with_low_header_sim'),
                user_prompt=load_prompt(role='user', task='annotate_questions_with_low_header_sim').format(
                    shots=input_data['shots'],
                    table_schema_set="\n".join([
                        table_serialization(
                            table_num=tdx + 1,
                            metadata=table['metadata'],
                            header=table['header'],
                            cell=None
                        )
                        for tdx, table in enumerate(input_data['gold_table_set'])
                    ]),
                    queries=" ".join(input_data['nl_query_set'])
                ),
                model_name=model_name,
                key=input_data['key']
            )
            for input_data in model_input
        ]
    ###

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks, desc=f"[{'OpenAI':<7}]"):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


def annotate_questions(
        table_lake: Dict[str, Dict[str, Any]],
        instance_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        load_shot: object,
        model_name: str,
        semaphore_value: int
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate questions

    [Params]
    table_lake         : Dict[str, Dict[str, Any]]
    instance_set       : List[Dict[str, Any]]
    classification     : Literal['high_header_sim', 'low_header_sim']
    load_shot          : object
    model_name         : str
    semaphore_value    : int

    [Returns]
    high_level_question_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initalization and setup
    high_level_question_set = [
        {
            'gold_table_id_set': instance['gold_table_id_set'], 
            'question_list': []
        } for instance in instance_set
    ] # Output

    for role in ['system', 'user']:
        task = f'annotate_questions_with_{classification}'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(instance_set):
        gold_table_set = [table_lake[t_id] for t_id in instance['gold_table_id_set']]

        ###
        if classification == 'high_header_sim':
            are_headers_overlap = defaultdict(int)
            for table in gold_table_set:
                for col in table['header']:
                    are_headers_overlap[col] += 1

            are_cells_overlap = defaultdict(set)
            for table in gold_table_set:
                for cdx, col in enumerate(table['header']):
                    for rdx, _ in enumerate(table['cell']):
                        are_cells_overlap[(col, table['cell'][rdx][cdx])].add(table['id'])

            model_input.append({
                'gold_table_set': gold_table_set,
                'overlapping_headers': [
                    header
                    for header, overlap_cnt in are_headers_overlap.items()
                    if overlap_cnt == len(instance['gold_table_id_set'])
                ],
                'overlapping_cells': [
                    {
                        'col': col,
                        'value': value
                    }
                    for (col, value), table_ids in are_cells_overlap.items()
                    if len(table_ids) == len(instance['gold_table_id_set'])
                ],
                'key': idx,
                'shots': load_shot()
            })
        
        elif classification == 'low_header_sim':
            model_input.append({
                'gold_table_set': gold_table_set,
                'nl_query_set': [
                    data['nl_query']
                    for data in instance['data_list']
                    if len(data['entailed_table_id_set']) > 1
                ],
                'key': idx,
                'shots': load_shot()
            })
        ###
    
    semaphore = asyncio.Semaphore(semaphore_value)
    task_output_list, cost = asyncio.run(annotate_questions_task(
        model_input=model_input,
        classification=classification,
        model_name=model_name,
        semaphore=semaphore
    ))

    clear_storage(storage_path=f"buffer/{classification}/annotate_questions", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx = task_output['key']

        try:
            matches = re.findall(r"Question (\d+): (.+)", task_output['response'])

            high_level_question_set[idx]['question_list'].extend([
                question.strip()
                for _, question in matches
            ])

            success_cnt += 1
        
        except:
            with open(f'buffer/{classification}/annotate_questions/{idx+1}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1

    # Buffer
    with open(f'buffer/{classification}/annotate_questions.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return high_level_question_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.annotate_questions':
    from utils.display import clear_storage, table_serialization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt