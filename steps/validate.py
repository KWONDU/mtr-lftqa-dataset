import asyncio
import json
import re
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple


async def validate_task(
        model_input: List[Dict[str, Any]],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: validate

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
            system_prompt=load_prompt(role='system', task=f"validate_{input_data['task_type']}"),
            user_prompt=load_prompt(role='user', task=f"validate_{input_data['task_type']}").format(
                gold_table_set_information="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=gold_table['cell']
                    )
                    for tdx, gold_table in enumerate(input_data['gold_table_set'])
                ]),
                question=input_data['question']
            ) if input_data['task_type'] == 'table_and_question' else \
            load_prompt(role='user', task=f"validate_{input_data['task_type']}").format(
                gold_table_set_information="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=gold_table['cell']
                    )
                    for tdx, gold_table in enumerate(input_data['gold_table_set'])
                ]),
                answer=input_data['answer']
            ) if input_data['task_type'] == 'table_and_answer' else \
            load_prompt(role='user', task=f"validate_{input_data['task_type']}").format(
                question=input_data['question'],
                answer=input_data['answer']
            ) if input_data['task_type'] == 'question_and_answer' else None, # Raise error
            model_name=model_name,
            key=(input_data['key'][0], input_data['key'][1], input_data['task_type']) # (idx, jdx, task_type)
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


def validate(
        table_lake: Dict[str, Dict[str, Any]],
        high_level_qa_pair_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        batch_size: int
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: validate

    [Params]
    table_lake             : Dict[str, Dict[str, Any]]
    high_level_qa_pair_set : List[Dict[str, Any]]
    classification         : Literal['high_header_sim', 'low_header_sim']
    model_name             : str
    batch_size             : int

    [Returns]
    high_level_qa_pair_set_with_validation : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    high_level_qa_pair_set_with_validation =  [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': qa_pair['question'],
                    'answer': qa_pair['answer'],
                    'validation': {
                        'table_and_question': None,
                        'table_and_question_reason': None,
                        'table_and_answer': None,
                        'table_and_answer_reason': None,
                        'question_and_answer': None,
                        'question_and_answer_reason': None
                    }
                }
                for qa_pair in instance['annotation']
            ]
        }
        for instance in high_level_qa_pair_set
    ] # Output

    for role in ['system', 'user']:
        for task_type in ['table_and_question', 'table_and_answer', 'question_and_answer']:
            save_prompt(file_path=f'prompts/{role}/validate_{task_type}.txt', role=role, task=f'validate_{task_type}')

    # Main task
    model_input = []
    for idx, instance in enumerate(high_level_qa_pair_set):
        gold_table_set = [
            table_lake[t_id]
            for t_id in instance['gold_table_id_set']
        ]

        for jdx, qa_pair in enumerate(instance['annotation']):
            for task_type in ['table_and_question', 'table_and_answer', 'question_and_answer']:
                model_input.append({
                    'task_type': task_type,
                    'gold_table_set': gold_table_set,
                    'question': qa_pair['question'],
                    'answer': qa_pair['answer'],
                    'key': (idx, jdx)
                })
    
    semaphore = asyncio.Semaphore(batch_size)
    task_output_list, cost = asyncio.run(validate_task(
        model_input=model_input,
        model_name=model_name,
        semaphore=semaphore
    ))

    clear_storage(storage_path=f"buffer/{classification}/validate", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx, task_type = task_output['key']

        try:
            validation = re.search(r"Validation: (Yes|No)", task_output['response']).group(1)
            reason = re.search(r"Reason: (.+)", task_output['response'], re.DOTALL).group(1)

            if validation.strip().title() == 'Yes':
                high_level_qa_pair_set_with_validation[idx]['annotation'][jdx]['validation'][task_type] = True
            elif validation == 'No':
                high_level_qa_pair_set_with_validation[idx]['annotation'][jdx]['validation'][task_type] = False
            else:
                raise ValueError("Unvalid response.")
            high_level_qa_pair_set_with_validation[idx]['annotation'][jdx]['validation'][f'{task_type}_reason'] = reason

            success_cnt += 1
        
        except:
            with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_{task_type}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1
    
    # Buffer
    with open(f'buffer/{classification}/validate.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    
    return high_level_qa_pair_set_with_validation, success_cnt, fail_cnt, cost


if __name__ == 'steps.validate':
    from utils.display import clear_storage, table_serialization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
