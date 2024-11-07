import asyncio
import json
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple
    

async def annotate_answer_task(
        model_input: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: annotate answer

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
                system_prompt=load_prompt(role='system', task='annotate_answer_with_high_header_sim'),
                user_prompt=load_prompt(role='user', task='annotate_answer_with_high_header_sim').format(
                    document_set="\n".join([
                        f"Document {ddx + 1} [title] {doc['title']} [content] {doc['content']}"
                        for ddx, doc in enumerate(input_data['document_set'])
                    ]),
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


def annotate_answer(
        table_lake: Dict[str, Dict[str, Any]],
        relevant_data_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        semaphore_value: int
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate answer

    [Params]
    table_lake              : Dict[str, Dict[str, Any]]
    relevant_data_set : List[Dict[str, Any]]
    classification          : Literal['high_header_sim', 'low_header_sim']
    model_name              : str
    semaphore_value         : int

    [Returns]
    high_level_qa_pair_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    high_level_qa_pair_set =  [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': data['question'],
                    'answer': ""
                }
                for data in instance['annotation']
            ]
        }
        for instance in relevant_data_set
    ] # Output

    for role in ['system', 'user']:
        task = f'annotate_answer_with_{classification}'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(relevant_data_set):
        ###
        if classification == 'high_header_sim':
            for jdx, data in enumerate(instance['annotation']):
                model_input.append({
                    'document_set': [
                        {
                            'title': table_lake[info['table_id']]['metadata'],
                            'content': info['information']
                        }
                        for info in data['relevant_data_set']
                    ],
                    'question': data['question'],
                    'key': (idx, jdx)
                })
        
        elif classification == 'low_header_sim':
            None
        ###
    
    semaphore = asyncio.Semaphore(semaphore_value)
    task_output_list, cost = asyncio.run(annotate_answer_task(
        model_input=model_input,
        classification=classification,
        model_name=model_name,
        semaphore=semaphore
    ))

    clear_storage(storage_path=f"buffer/{classification}/annotate_answer", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx = task_output['key']

        try:
            high_level_qa_pair_set[idx]['annotation'][jdx]['answer'] = task_output['response'].strip().replace('\n', ' ')

            success_cnt += 1
        
        except:
            with open(f'buffer/{classification}/annotate_answer/{idx+1}_{jdx+1}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1

    # Buffer
    with open(f'buffer/{classification}/annotate_answer.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return high_level_qa_pair_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.annotate_answer':
    from utils.display import clear_storage
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
