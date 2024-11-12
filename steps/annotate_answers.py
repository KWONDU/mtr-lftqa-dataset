import asyncio
import json
import re
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Tuple


async def annotate_answers_task(
        model_input: List[Dict[str, Any]],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: annotate answers

    [Params]
    model_input    : List[Dict[str, Any]]
    model_name     : str
    semaphore      : asyncio.Semaphore

    [Return]
    model_output_list : List[Dict[str, Any]]
    cost              : int
    """
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_answers_with_low_header_sim'),
            user_prompt=load_prompt(role='user', task='annotate_answers_with_low_header_sim').format(
                table_set="\n".join([
                    table_serialization(
                        table_num=tdx+1,
                        metadata=table['metadata'],
                        header=table['header'],
                        cell=table['cell']
                    )
                    for tdx, table in enumerate(input_data['table_set'])
                ]),
                nl_document=" ".join(input_data['nl_document_list'])
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for input_data in model_input
    ]

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


def annotate_answers(
        table_lake: Dict[str, Dict[str, Any]],
        table_document_set: List[Dict[str, Any]],
        model_name: str,
        batch_size: int
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate answers

    [Params]
    table_lake     : Dict[str, Dict[str, Any]]
    table_document_set   : List[Dict[str, Any]]
    model_name     : str
    batch_size     : int

    [Returns]
    high_level_answer_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initalization and setup
    high_level_answer_set = [
        {
            'gold_table_id_set': instance['table_id_set'],
            'answer_list': []
        }
        for instance in table_document_set
    ] # Output

    for role in ['system', 'user']:
        task = 'annotate_answers_with_low_header_sim'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(table_document_set):
        model_input.append({
            'table_set': [table_lake[table_id] for table_id in instance['table_id_set']],
            'nl_document_list': instance['nl_document_list'],
            'key': idx
        })

    semaphore = asyncio.Semaphore(batch_size)
    task_output_list, cost = asyncio.run(annotate_answers_task(
        model_input=model_input,
        model_name=model_name,
        semaphore=semaphore
    ))

    clear_storage(storage_path="buffer/low_header_sim/annotate_answers", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx = task_output['key']

        try:
            matches = re.findall(r"Answer (\d+): (.+)", task_output['response'])

            high_level_answer_set[idx]['answer_list'].extend([
                answer.strip().replace('\n', ' ')
                for _, answer in matches
            ])

            success_cnt += 1
        
        except:
            with open(f'buffer/low_header_sim/annotate_answers/{idx+1}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1

    # Buffer
    with open('buffer/low_header_sim/annotate_answers.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return high_level_answer_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.annotate_answers':
    from utils.display import clear_storage, table_serialization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
