import asyncio
import json
import re
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Tuple


async def annotate_questions_task(
        semaphore: asyncio.Semaphore,
        model_input: List[Dict[str, Any]],
        model_name: str
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: annotate questions

    [Params]
    semaphore   : asyncio.Semaphore
    model_input : List[Dict[str, Any]]
    model_name  : str

    [Return]
    model_output_list : List[Dict[str, Any]]
    cost              : int
    """
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_questions_with_low_header_sim'),
            user_prompt=load_prompt(role='user', task=f'annotate_questions_with_low_header_sim').format(
                shots=input_data['shots'],
                gold_table_set_schema="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=None
                    )
                    for tdx, gold_table in enumerate(input_data['gold_table_set'])
                ]),
                nl_query_list="\n".join([
                    f"Query {ddx + 1}: {nl_query}"
                    for ddx, nl_query in enumerate(input_data['nl_query_list'])
                ])
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


def annotate_questions(
        table_lake: Dict[str, Dict[str, Any]],
        instance_set: List[Dict[str, Any]],
        load_shot: object,
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate questions

    [Params]
    table_lake   : Dict[str, Dict[str, Any]]
    instance_set : List[Dict[str, Any]]
    load_shot    : object
    model_name   : str
    semaphore    : asyncio.Semaphore

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
    ]

    for role in ['system', 'user']:
        task = f'annotate_questions_with_low_header_sim'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(instance_set):
        gold_table_set = [table_lake[t_id] for t_id in instance['gold_table_id_set']]
        data_list = instance['data_list']

        model_input.append({
            'gold_table_set': gold_table_set,
            'nl_query_list': [
                data['nl_query']
                for data in data_list
                if len(data['entailed_table_id_set']) > 1 # only consider multi-table related data
            ],
            'key': idx,
            'shots': load_shot()
        })
    
    task_output_list, cost = asyncio.run(annotate_questions_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=model_name
    ))

    # Clear
    clear_storage(storage_path=f'buffer/low_header_sim/annotate_questions', extension='txt')

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx = task_output['key']

        file_buffer = "# Gold table set information"
        for tdx, table_id in enumerate(instance_set[idx]['gold_table_id_set']):
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

        file_buffer = "\n".join([file_buffer, "# Entailed NL query list"])
        file_buffer = "\n".join([
            file_buffer,
            "\n".join([
                f"Query {ddx + 1}: {data['nl_query']}"
                for ddx, data in enumerate(instance_set[idx]['data_list'])
                if len(data['entailed_table_id_set']) > 1 # only consider multi-table related data
            ]),
            ""
        ])

        try:
            indices = re.findall(r"Annotated question (\d+):", task_output['response'])
            questions = re.findall(r"Question: (.+?)(?=Type:)", task_output['response'], re.DOTALL)
            types = re.findall(r"Type: (\w+)", task_output['response'])

            questions = [question.strip() for question in questions]

            high_level_question_set[idx]['question_list'].extend(questions)
            for qdx, question, type in zip(indices, questions, types):
                file_buffer = "\n".join([
                    file_buffer,
                    "\n".join([
                        f"# {qdx}th annotation:",
                        f"High-level question: {question}",
                        f"Question type: {type}",
                        ""
                    ])
                ])

            success_cnt += 1
            with open(f'buffer/low_header_sim/annotate_questions/{idx+1}_successed.txt', 'w') as file:
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
            with open(f'buffer/low_header_sim/annotate_questions/{idx+1}_failed.txt', 'w') as file:
                file.write(file_buffer)

    # Buffer
    with open('buffer/low_header_sim/annotate_questions.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return high_level_question_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.low_header_sim.annotate_questions':
    from utils.display import clear_storage, table_serialization, table_visualization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
