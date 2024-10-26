import asyncio
import json
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple


async def validate_task(semaphore, model_input, model_name):
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
            ) if input_data['task_type'] == 'question_and_answer' else None,
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
        classification: Literal['low_header_sim', 'high_header_sim'],
        table_lake: Dict[str, Dict[str, Any]],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: validate

    [Params]
    classification         : Literal['low_header_sim', 'high_header_sim']
    table_lake   : Dict[str, Dict[str, Any]]
    model_name   : str
    semaphore    : asyncio.Semaphore

    [Returns]
    high_level_qa_pair_set_with_score : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initialization and setup
    with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'r') as file:
        high_level_qa_pair_set = json.load(file)

    high_level_qa_pair_set_with_validation =  [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': qa_pair['question'],
                    'answer': qa_pair['answer'],
                    'validation': {
                        'table_and_question': None,
                        'table_and_answer': None,
                        'question_and_answer': None
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
    
    semaphore = asyncio.Semaphore(100)
    task_output_list, cost = asyncio.run(validate_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=model_name
    ))

    # Clear
    clear_storage(storage_path=f'buffer/{classification}/validate', extension='txt')

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx, task_type = task_output['key']

        if 'table' in task_type:
            file_buffer = "# Gold table set"
            for tdx, table_id in enumerate(high_level_qa_pair_set[idx]['gold_table_id_set']):
                file_buffer = "\n".join([
                    file_buffer,
                    table_visualization(
                        table_num=tdx+1,
                        metadata=table_lake[table_id]['metadata'],
                        header=table_lake[table_id]['header'],
                        cell=table_lake[table_id]['cell']
                    ),
                    ""
                ])
            file_buffer = "\n".join([
                file_buffer,
                f"# Question: {high_level_qa_pair_set[idx]['annotation'][jdx]['question']}" if 'question' in task_type else \
                f"# Answer: {high_level_qa_pair_set[idx]['annotation'][jdx]['answer']}" if 'answer' in task_type else None,
                ""
            ])
        else:
            file_buffer = "\n".join([
                f"# Question: {high_level_qa_pair_set[idx]['annotation'][jdx]['question']}",
                "",
                f"# Answer: {high_level_qa_pair_set[idx]['annotation'][jdx]['answer']}",
                ""
            ])

        try:
            high_level_qa_pair_set_with_validation[idx]['annotation'][jdx]['validation'].update({
                task_type: True if task_output['response'].strip().title() == 'Yes' else \
                    False if task_output['response'].strip().title() == 'No' else None
            })

            file_buffer = "\n".join([
                file_buffer,
                f"Validation: {task_output['response']}",
                ""
            ])

            success_cnt += 1
            if task_type == 'table_and_question':
                with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_t_and_q_successed.txt', 'w') as file:
                    file.write(file_buffer)
            elif task_type == 'table_and_answer':
                with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_t_and_a_successed.txt', 'w') as file:
                    file.write(file_buffer)
            elif task_type == 'question_and_answer':
                with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_q_and_a_successed.txt', 'w') as file:
                    file.write(file_buffer)
            else:
                continue
        
        except:
            file_buffer = "\n".join([
                file_buffer,
                "# Response",
                task_output['response'],
                "",
                "# Exception",
                traceback.format_exc(),
                ""
            ])

            fail_cnt += 1
            if task_type == 'table_and_question':
                with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_t_and_q_failed.txt', 'w') as file:
                    file.write(file_buffer)
            elif task_type == 'table_and_answer':
                with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_t_and_a_failed.txt', 'w') as file:
                    file.write(file_buffer)
            elif task_type == 'question_and_answer':
                with open(f'buffer/{classification}/validate/{idx+1}_{jdx+1}_q_and_a_failed.txt', 'w') as file:
                    file.write(file_buffer)
            else:
                continue
    
    # Buffer
    with open(f'buffer/{classification}/validate.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    
    return high_level_qa_pair_set_with_validation, success_cnt, fail_cnt, cost


if __name__ == 'steps.validate':
    from utils.display import clear_storage, table_serialization, table_visualization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
