import asyncio
import json
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Tuple
    

async def annotate_question_task(
        model_input: List[Dict[str, Any]],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: annotate question

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
            system_prompt=load_prompt(role='system', task='annotate_question_with_low_header_sim'),
            user_prompt=load_prompt(role='user', task='annotate_question_with_low_header_sim').format(
                table_schema_set="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=table['metadata'],
                        header=None,
                        cell=None
                    )
                    for tdx, table in enumerate(input_data['gold_table_set'])
                ]),
                answer=input_data['answer']
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


def annotate_question(
        table_lake: Dict[str, Dict[str, Any]],
        high_level_answer_set: List[Dict[str, Any]],
        model_name: str,
        batch_size: int
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate question

    [Params]
    table_lake              : Dict[str, Dict[str, Any]]
    high_level_answer_set : List[Dict[str, Any]]
    model_name              : str
    batch_size              : int

    [Returns]
    high_level_qa_pair_set : List[Dict[str, Any]]
    success_cnt            : int
    fail_cnt               : int
    cost                   : int
    """
    high_level_qa_pair_set =  [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': "",
                    'answer': answer
                }
                for answer in instance['answer_list']
            ]
        }
        for instance in high_level_answer_set
    ] # Output

    for role in ['system', 'user']:
        task = 'annotate_question_with_low_header_sim'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(high_level_answer_set):
        for jdx, answer in enumerate(instance['answer_list']):
            model_input.append({
                'gold_table_set': [table_lake[table_id] for table_id in instance['gold_table_id_set']],
                'answer': answer,
                'key': (idx, jdx)
            })
    
    semaphore = asyncio.Semaphore(batch_size)
    task_output_list, cost = asyncio.run(annotate_question_task(
        model_input=model_input,
        model_name=model_name,
        semaphore=semaphore
    ))

    clear_storage(storage_path="buffer/low_header_sim/annotate_question", extension="txt")

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx = task_output['key']

        try:
            high_level_qa_pair_set[idx]['annotation'][jdx]['question'] = task_output['response'].strip().replace('\n', ' ')

            success_cnt += 1
        
        except:
            with open(f'buffer/low_header_sim/annotate_question/{idx+1}_{jdx+1}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1

    # Buffer
    with open('buffer/low_header_sim/annotate_question.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return high_level_qa_pair_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.annotate_question':
    from utils.display import clear_storage, table_serialization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
