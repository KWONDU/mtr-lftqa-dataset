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


async def generate_text2sql_statement(model_input, model_name, semaphore, table_serialization, get_async_openai_response):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='generate_text2sql_statement'),
            user_prompt=load_prompt(role='user', task='generate_text2sql_statement').format(
                shots=shots_prompt(),
                nl_query=input_data['nl_query'],
                sql_query_result=table_serialization(
                    table_num=-1,
                    metadata=None,
                    header=input_data['sql_query_result']['header'],
                    cell=input_data['sql_query_result']['cell']
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
    from utils.display import table_serialization
    from utils.openai import load_prompt, save_prompt, get_async_openai_response
    return table_serialization, load_prompt, save_prompt, get_async_openai_response


if __name__ == '__main__':
    table_serialization, load_prompt, save_prompt, get_async_openai_response = import_utils()

    data_set_with_generated_statement = defaultdict(list)

    for split in ['train', 'validation']:
        with open(f'storage/modified_multitabqa_{split}_set.json', 'r') as file:
            split_set = json.load(file)
        
        for role in ['system', 'user']:
            save_prompt(file_path=f'prompts/{role}/generate_text2sql_statement.txt', role=role, task='generate_text2sql_statement')
        
        # Generate statement from each NL-SQL query pair and SQL query result

        model_name = 'gpt-3.5-turbo'

        ###

        # Remove duplication data using SQL query result for each gold table id set
        task_input = []
        for idx, instance in enumerate(split_set):
            unique_data = set()

            for jdx, data in enumerate(instance['data_list']):
                sql_query_result_serialization = table_serialization(
                        table_num=-1,
                        metadata=None,
                        header=data['sql_query_result']['header'],
                        cell=data['sql_query_result']['cell']
                    )
                
                if sql_query_result_serialization in unique_data:
                    continue

                unique_data.add(sql_query_result_serialization)
                task_input.append(
                    {
                        'nl_query': data['nl_query'],
                        'sql_query_result': data['sql_query_result'],
                        'key': (idx, jdx)
                    }
                )
        
        semaphore = asyncio.Semaphore(100)
        task_output_list, cost = asyncio.run(generate_text2sql_statement(
            model_input=task_input,
            model_name=model_name,
            semaphore=semaphore,
            table_serialization=table_serialization,
            get_async_openai_response=get_async_openai_response
        ))

        print(f'Total {split} set cost: ${cost:.2f}')

        with open(f'buffer/generate_text2sql_statement_{split}_set_llm.json', 'w') as file:
            json.dump(task_output_list, file, indent=4)
        
        task_response_list = [
            (task_output['key'], task_output['response'])
            for task_output in task_output_list
        ]

        for key, response in task_response_list:
            idx, jdx = key

            gold_table_id_set = split_set[idx]['gold_table_id_set']
            input_data = split_set[idx]['data_list'][jdx]

            data_set_with_generated_statement[tuple(gold_table_id_set)].append(
                {
                    'split': split,
                    'nl_query': input_data['nl_query'],
                    'sql_query': input_data['sql_query'],
                    'sql_query_result': input_data['sql_query_result'],
                    'statement': response
                }
            )
    
    with open('storage/data_set_with_generated_statement.json', 'w') as file:
        json.dump(
            [
                {
                    'gold_table_id_set': gold_table_id_set,
                    'data_list': data_list
                }
                for gold_table_id_set, data_list
                in data_set_with_generated_statement.items()
            ]
            ,
            file,
            indent=4
        )
