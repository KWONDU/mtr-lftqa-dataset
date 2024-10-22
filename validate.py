import asyncio
import json
import re
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, List, Dict, Tuple
from utils.display import clear_storage, table_serialization, table_visualization
from utils.openai import get_async_openai_response, load_prompt, save_prompt


async def validate_task(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='validate'),
            user_prompt=load_prompt(role='user', task='validate').format(
                gold_table_set_information="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=gold_table['cell']
                    )
                    for tdx, gold_table in enumerate(input_data['gold_table_set'])
                ]),
                question=input_data['question'],
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


def validate(
        table_lake: Dict[str, Dict[str, Any]],
        load_shot: object,
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: validate

    [Params]
    table_lake   : Dict[str, Dict[str, Any]]
    load_shot    : object
    model_name   : str
    semaphore    : asyncio.Semaphore

    [Returns]
    high_level_qa_pair_set_with_score : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initialization and setup
    with open('results/storage/high_level_qa_pair_set.json', 'r') as file:
        high_level_qa_pair_set = json.load(file)

    high_level_qa_pair_set_with_score =  [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': qa_pair['question'],
                    'answer': qa_pair['answer'],
                    'score': {
                        'description': ('type', 'score')
                    }
                }
                for qa_pair in instance['annotation']
            ]
        }
        for instance in high_level_qa_pair_set
    ] # Output

    for role in ['system', 'user']:
        task = 'validate'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(high_level_qa_pair_set):
        gold_table_set = [
            table_lake[t_id]
            for t_id in instance['gold_table_id_set']
        ]

        for jdx, qa_pair in enumerate(instance['annotation']):
            model_input.append({
                'gold_table_set': gold_table_set,
                'question': qa_pair['question'],
                'answer': qa_pair['answer'],
                'key': (idx, jdx),
                'shot': load_shot()
            })
    
    semaphore = asyncio.Semaphore(100)
    task_output_list, cost = asyncio.run(validate_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=model_name
    ))

    # Clear
    clear_storage(storage_path='results/buffer/validate', extension='txt')

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx = task_output['key']

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
            f"# Question: {high_level_qa_pair_set[idx]['annotation'][jdx]['question']}",
            "",
            f"# Answer: {high_level_qa_pair_set[idx]['annotation'][jdx]['answer']}",
            ""
        ])

        try:
            scores = re.findall(r'score: (\d+)', task_output['response'])
            if len(scores) != 7:
                raise ValueError(f"[Error] only extract {len(scores)} scores.")
            
            high_level_qa_pair_set_with_score[idx]['annotation'][jdx]['score'].update({
                'gold_table_set': [
                    ('relevance', int(scores[0]))
                ],
                'annotated_question': [
                    ('focus', int(scores[1])),
                    ('comprehensiveness', int(scores[2]))
                ],
                'annotated_answer': [
                    ('fluency', int(scores[3])),
                    ('coherence', int(scores[4])),
                    ('faithfulness', int(scores[5])),
                    ('comprehensiveness', int(scores[6]))
                ]
            })

            file_buffer = "\n".join([
                file_buffer,
                "# About gold table set",
                f"Relevance score: {scores[0]}",
                "",
                "# About annotated question",
                f"Focus score: {scores[1]}",
                f"Comprehensiveness score: {scores[2]}",
                "",
                "# About annotated answer",
                f"Fluency score: {scores[3]}",
                f"Coherence score: {scores[4]}",
                f"Faithfulness score: {scores[5]}",
                f"Comprehensiveness score: {scores[6]}"
                ""
            ])

            success_cnt += 1
            with open(f'results/buffer/validate/{idx+1}_{jdx+1}_successed.txt', 'w') as file:
                file.write(file_buffer)
        
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
            with open(f'results/buffer/validate/{idx+1}_{jdx+1}_failed.txt', 'w') as file:
                file.write(file_buffer)

    # Buffer
    with open('results/buffer/validate.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    
    return high_level_qa_pair_set_with_score, success_cnt, fail_cnt, cost
