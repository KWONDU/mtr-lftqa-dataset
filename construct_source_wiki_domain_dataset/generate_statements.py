import asyncio
import json
import sys
import os
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio


def shots_prompt():
    with open('shots/shots.json', 'r') as file:
        shots = json.load(file)
    
    return "\n\n".join([
        "\n".join(
            [
                f"Example {i + 1}:",
                f"NL query: {shot['nl_query']}",
                f"SQL query result: {table_serialization(table_num=-1, metadata=None, header=shot['sql_query_result']['header'], cell=shot['sql_query_result']['cell'])}",
                f"Statement: {shot['statement']}"
            ]
        )
        for i, shot in enumerate(shots)
    ])


async def generate_wiki_domain_statement(model_input, model_name, semaphore, table_serialization, get_async_openai_response):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='generate_wiki_domain_statement'),
            user_prompt=load_prompt(role='user', task='generate_wiki_domain_statement').format(
                shots=shots_prompt(),
                nl_query=input_data['nl_query'],
                sql_query_result=table_serialization(
                    table_num=-1,
                    metadata=None,
                    header=None,
                    cell=[input_data['sql_query_result']] # 2d list
                )
            ),
            model_name=model_name,
            key=input_data['key']
        ) for input_data in model_input
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks):
        try:
            model_output = await task
            model_output_list.append(model_output)
        except Exception as e:
            print(e)

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    from utils.display import table_serialization
    from utils.openai import load_prompt, save_prompt, get_async_openai_response
    return load_dataset, table_serialization, load_prompt, save_prompt, get_async_openai_response


if __name__ == '__main__':
    load_dataset, table_serialization, load_prompt, save_prompt, get_async_openai_response = import_utils()

    for role in ['system', 'user']:
        save_prompt(file_path=f'prompts/{role}/generate_wiki_domain_statement.txt', role=role, task='generate_wiki_domain_statement')
    
    # Generate statement for full dataset

    model_name = 'gpt-3.5-turbo'

    task_input = [
        {
            'nl_query': data['question'],
            'sql_query_result': next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type == 'word'),
            'key': idx
        }
        for idx, data in enumerate(load_dataset(dataset_name='Open-WikiTable')[:])
    ]

    semaphore = asyncio.Semaphore(100)
    task_output_list, cost = asyncio.run(generate_wiki_domain_statement(
        model_input=task_input,
        model_name=model_name,
        semaphore=semaphore,
        table_serialization=table_serialization,
        get_async_openai_response=get_async_openai_response
    ))

    print(f'Total cost: ${cost:.2f}')

    with open(f'buffer/generate_wiki_domain_statement_llm.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    
    generated_statement_set = [
        task_output['response']
        for task_output in sorted(task_output_list, key=lambda x: x['key'])
    ]

    with open('storage/generated_statements.json', 'w') as file:
        json.dump(generated_statement_set, file, indent=4)
