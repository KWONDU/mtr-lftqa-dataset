import asyncio
import json
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple
    

async def extract_information_task(
        semaphore: asyncio.Semaphore,
        model_input: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: extract information

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
                system_prompt=load_prompt(role='system', task='extract_information_with_high_header_sim'),
                user_prompt=load_prompt(role='user', task='extract_information_with_high_header_sim').format(
                    table_document=table_serialization(
                            table_num=-1,
                            metadata=input_data['table']['metadata'],
                            header=input_data['table']['header'],
                            cell=input_data['table']['cell']
                        ) + f" [document]: {' '.join(input_data['nl_document_list'])}",
                    question=input_data['question']
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


def extract_information(
        table_lake: Dict[str, Dict[str, Any]],
        table_document_set: List[Dict[str, Any]],
        high_level_question_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: extract information

    [Params]
    table_lake              : Dict[str, Dict[str, Any]]
    table_document_set      : List[Dict[str, Any]]
    high_level_question_set : List[Dict[str, Any]]
    classification          : Literal['high_header_sim', 'low_header_sim']
    model_name              : str
    semaphore               : asyncio.Semaphore

    [Returns]
    related_information_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    related_information_set = [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': question,
                    'information_set': [
                        {
                            'table_id': table_id,
                            'information': ""
                        }
                        for table_id in instance['gold_table_id_set']
                    ]
                }
                for question in instance['question_list']
            ]
        }
        for instance in high_level_question_set
    ] # Output

    for role in ['system', 'user']:
        task = f'extract_information_with_{classification}'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(high_level_question_set):
        ###
        if classification == 'high_header_sim':
            for jdx, question in enumerate(instance['question_list']):
                for kdx, table_id in enumerate(instance['gold_table_id_set']):
                    model_input.append({
                        'table': table_lake[table_id],
                        'nl_document_list': next(
                            (
                                tb_doc['nl_document_list']
                                for tb_doc in table_document_set
                                if tb_doc['table_id'] == table_id
                            ), []
                        ),
                        'question': question,
                        'key': (idx, jdx, kdx)
                    })
        
        elif classification == 'low_header_sim':
            None
        ###
    
    task_output_list, cost = asyncio.run(extract_information_task(
        semaphore=semaphore,
        model_input=model_input,
        classification=classification,
        model_name=model_name
    ))

    clear_storage(storage_path=f"buffer/{classification}/extract_information", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx, kdx = task_output['key']
        question = high_level_question_set[idx]['question_list'][jdx]

        try:
            related_information_set[idx]['annotation'][jdx]['information_set'][kdx]['information'] = task_output['response'].strip().replace('\n', ' ')

            success_cnt += 1
        
        except:
            with open(f'buffer/{classification}/extract_information/{idx+1}_{jdx+1}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1

    # Buffer
    with open(f'buffer/{classification}/extract_information.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return related_information_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.extract_information':
    from utils.display import clear_storage, table_serialization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
