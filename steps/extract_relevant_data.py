import asyncio
import json
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple


async def extract_relevant_data_task(
        model_input: List[Dict[str, Any]],
        model_name: str,
        classification: Literal['high_header_sim', 'low_header_sim'],
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: extract relevant data

    [Params]
    model_input    : List[Dict[str, Any]]
    model_name     : str
    classification : Literal['high_header_sim', 'low_header_sim']
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
                system_prompt=load_prompt(role='system', task='extract_relevant_data_with_high_header_sim'),
                user_prompt=load_prompt(role='user', task='extract_relevant_data_with_high_header_sim').format(
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
        tasks = [
            get_async_openai_response(
                semaphore=semaphore,
                system_prompt=load_prompt(role='system', task='extract_relevant_data_with_low_header_sim'),
                user_prompt=load_prompt(role='user', task='extract_relevant_data_with_low_header_sim').format(
                    header="Columns are " + ", ".join(input_data['header']) + ".",
                    question=input_data['question']
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


def extract_relevant_data(
        table_lake: Dict[str, Dict[str, Any]],
        table_document_set: List[Dict[str, Any]],
        high_level_question_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        batch_size: int
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: extract relevant data

    [Params]
    table_lake              : Dict[str, Dict[str, Any]]
    table_document_set      : List[Dict[str, Any]]
    high_level_question_set : List[Dict[str, Any]]
    classification          : Literal['high_header_sim', 'low_header_sim']
    model_name              : str
    batch_size              : int

    [Returns]
    relevant_data_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    relevant_data_set = [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': question,
                    'relevant_data_set': []
                }
                for question in instance['question_list']
            ]
        }
        for instance in high_level_question_set
    ] # Output

    for role in ['system', 'user']:
        task = f'extract_relevant_data_with_{classification}'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)
    
    # Main task
    model_input = []
    for idx, instance in enumerate(high_level_question_set):
        for jdx, question in enumerate(instance['question_list']):
            ###
            if classification == 'high_header_sim':
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
                with open('additional_process_source_spidertableqa_dataset/storage/joined_table_set.json', 'r') as file:
                    joined_table_set = json.load(file)
                
                joined_table = next(tb for tb in joined_table_set if tb['table_id_set'] == instance['gold_table_id_set'])
                model_input.append({
                    'header': joined_table['header'],
                    'question': question,
                    'key': (idx, jdx)
                })
            ###

    semaphore = asyncio.Semaphore(batch_size)
    task_output_list, cost = asyncio.run(extract_relevant_data_task(
        model_input=model_input,
        model_name=model_name,
        classification=classification,
        semaphore=semaphore
    ))

    clear_storage(storage_path=f"buffer/{classification}/extract_relevant_data", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0

    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        try:
            ###
            if classification == 'high_header_sim':
                idx, jdx, kdx = task_output['key']
                question = high_level_question_set[idx]['question_list'][jdx]
            
                relevant_data_set[idx]['annotation'][jdx]['relevant_data_set'].append(
                    {
                        'table_id': high_level_question_set[idx]['gold_table_id_set'][kdx],
                        'information': task_output['response'].strip().replace('\n', ' ')
                    }
                )
            
            elif classification == 'low_header_sim':
                idx, jdx = task_output['key']
                question = high_level_question_set[idx]['question_list'][jdx]

                # Already load
                joined_table = next(tb for tb in joined_table_set if tb['table_id_set'] == high_level_question_set[idx]['gold_table_id_set'])

                relevant_columns = [col.strip() for col in task_output['response'].split(',')]
                relevant_cdx_list = [
                    cdx
                    for cdx, col in enumerate(joined_table['header'])
                    if col in relevant_columns
                ]

                relevant_header = [joined_table['header'][cdx] for cdx in relevant_cdx_list]
                unique_relevant_rows = set(tuple(row[cdx] for cdx in relevant_cdx_list) for row in joined_table['cell'])

                relevant_data_set[idx]['annotation'][jdx]['relevant_data_set'] = {
                    'joined_table': {
                        'header': relevant_header,
                        'cell': [list(row) for row in unique_relevant_rows]
                    },
                    'nl_document_list': next(
                        (
                            tb_doc['nl_document_list']
                            for tb_doc in table_document_set
                            if tb_doc['table_id_set'] == high_level_question_set[idx]['gold_table_id_set']
                        ), []
                    )
                }
            ###
            
            success_cnt += 1
        
        except:
            ###
            if classification == 'high_header_sim':
                with open(f'buffer/{classification}/extract_relevant_data/{idx+1}_{jdx+1}_{kdx+1}_error.txt', 'w') as file:
                    file.write(traceback.format_exc())
            
            elif classification == 'low_header_sim':
                with open(f'buffer/{classification}/extract_relevant_data/{idx+1}_{jdx+1}_error.txt', 'w') as file:
                    file.write(traceback.format_exc())
            ###

            fail_cnt += 1
    
    # Buffer
    with open(f'buffer/{classification}/extract_relevant_data.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return relevant_data_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.extract_relevant_data':
    from utils.display import clear_storage, table_serialization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
