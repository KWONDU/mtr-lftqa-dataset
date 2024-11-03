import asyncio
import json
import re
import traceback
from collections import defaultdict
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple


async def annotate_questions_task(
        semaphore: asyncio.Semaphore,
        model_input: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: annotate questions

    [Params]
    semaphore      : asyncio.Semaphore
    model_input    : List[Dict[str, Any]]
    classification : Literal['high_header_sim', 'low_header_sim']
    model_name     : str

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
                    gold_table_set_titles="\n".join([
                        table_serialization(
                            table_num=tdx + 1,
                            metadata=table['metadata'],
                            header=None,
                            cell=None
                        )
                        for tdx, table in enumerate(input_data['gold_table_set']) 
                    ]),
                    statement_pattern_set=" ".join(input_data['statement_pattern_set']),
                    table_headers="Table headers are " + ", ".join(input_data['table_headers']) + ".",
                    overlapping_values=", ".join([
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
        None
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


def regularize_pattern(pattern, table_metadata_list):
    regularized_pattern = pattern.replace('{', '').replace('}', '').replace('\"', '')

    table1_titles = table_metadata_list[0].split('|')
    table2_titles = table_metadata_list[1].split('|')

    flag = -1
    for idx, (title1, title2) in enumerate(zip(table1_titles, table2_titles)):
        if title1 != title2:
            flag = idx
            break # page | section | table
    
    if flag == -1:
        return regularized_pattern
    
    for table_metadata in table_metadata_list:
        title = table_metadata.split('|')[flag].strip()
        for word in title.split(' '):
            regularized_pattern = regularized_pattern.replace(word.strip(), "\{gold_table_set_titles\}")
    
    return regularized_pattern


def annotate_questions(
        table_lake: Dict[str, Dict[str, Any]],
        instance_set: List[Dict[str, Any]],
        table_document_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        load_shot: object,
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate questions

    [Params]
    table_lake         : Dict[str, Dict[str, Any]]
    instance_set       : List[Dict[str, Any]]
    table_document_set : List[Dict[str, Any]]
    classification     : Literal['high_header_sim', 'low_header_sim']
    load_shot          : object
    model_name         : str
    semaphore          : asyncio.Semaphore

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
                        are_cells_overlap[(col.lower().strip(), table['cell'][rdx][cdx])].add(table['id']) # don't modify table cell

            model_input.append({
                'gold_table_set': gold_table_set,
                'statement_pattern_set': [
                    regularize_pattern(pattern, [tb['metadata'] for tb in gold_table_set])
                    for tb_doc in table_document_set
                    for t_id in instance['gold_table_id_set']
                    if t_id == tb_doc['table_id']
                    for pattern in tb_doc['statement_pattern_set']
                ],
                'table_headers': [
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
                'statement_pattern_set': [
                    pattern
                    for tb_doc in table_document_set
                    if instance['gold_table_id_set'] == tb_doc['table_id_set']
                    for pattern in tb_doc['statement_pattern_set']
                ],
                'key': idx
            })
        ###
    
    task_output_list, cost = asyncio.run(annotate_questions_task(
        semaphore=semaphore,
        model_input=model_input,
        classification=classification,
        model_name=model_name
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
