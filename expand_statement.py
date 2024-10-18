import asyncio
import json
import numpy as np
import pandas as pd
import random
import re
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from utils.dataset import load_source_dataset
from utils.display import table_visualization
from utils.openai import get_async_openai_response, load_prompt, save_prompt
from get_shots import get_expand_statement_task_shots


def clear_storage(storage_path):
    import glob
    import os

    storage_memory = glob.glob(os.path.join(storage_path, '*.txt'))
    for memory in storage_memory:
        try:
            os.remove(memory)
        except:
            continue
    
    return "[Done] clear storage."
    

async def expand_statement_task(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='expand_statement'),
            user_prompt=load_prompt(role='user', task='expand_statement').format(
                shots=input_data['shots'],
                df_caption=input_data['df_caption'],
                df_columns=" | ".join(input_data['df_columns']),
                df_first_row=' | '.join(input_data['df_first_row']),
                statement=input_data['statement']
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for input_data in model_input
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks, desc='Get OpenAI responses...'):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


MODEL_NAME = 'gpt-3.5-turbo'


def expand_statement_pattern():
    None


if __name__ == '__main__':
    ds = load_source_dataset(dataset_name='SourceWikipedia')

    table_lake = {tb['id']: tb for tb in ds.tables}

    random.seed(42)
    N = 10
    instance_set = random.sample(ds[:], N)

    table_document_set = [{'table_id': t_id, 'nl_document_list': []} for t_id, _ in table_lake.items()] # Output

    for role in ['system', 'user']:
        task = 'expand_statement'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    model_input = []
    for idx, instance in enumerate(instance_set):
        gold_table_set = [table_lake[t_id] for t_id in instance['gold_table_id_set']]
        data_list = instance['data_list']

        for jdx, data in enumerate(data_list):
            silver_table = next(tb for tb in gold_table_set if tb['id'] in data['entailed_table_id_set'])
            model_input.append({
                'df_caption': silver_table['metadata'],
                'df_columns': silver_table['header'],
                'df_first_row': silver_table['cell'][0],
                'statement': data['statement'],
                'key': (idx, jdx),
                'shots': get_expand_statement_task_shots()
            })
    
    semaphore = asyncio.Semaphore(100)
    task_output_list, cost = asyncio.run(expand_statement_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=MODEL_NAME
    ))

    ### CLEAR STORAGE ###
    print(clear_storage('storage_expand_statement/results'))
    ### CLEAR STORAGE ###

    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc='Get table document set...'):
        idx, jdx = task_output['key']
        table = next(table_lake[t_id] for t_id in instance_set[idx]['data_list'][jdx]['entailed_table_id_set'])

        table_document_set[idx]['nl_document_list'].append(instance_set[idx]['data_list'][jdx]['statement'])
        file_buffer = "\n".join([
            "# Table information",
            table_visualization(
                table_num=-1,
                metadata=table['metadata'],
                header=table['header'],
                cell=table['cell']
            ),
            "",
            "# Statement",
            instance_set[idx]['data_list'][jdx]['statement'],
            ""
        ])

        try:
            code = re.search(r"```python\s+([\s\S]+?)```", task_output['response']).group(1)
            exec(code)

            statement_pattern, expanded_statement_list = expand_statement_pattern(
                df=pd.DataFrame(data=np.array(table['cell']), columns=np.array(table['header']))
            )

            table_document_set[idx]['nl_document_list'].extend(expanded_statement_list)
            file_buffer = "\n".join([
                file_buffer,
                "# Statement pattern",
                statement_pattern,
                "# Expanded staetments",
                "\n".join([expanded_statement for expanded_statement in expanded_statement_list]),
                "",
                "# Python code",
                task_output['response'],
                ""
            ])

            success_cnt += 1
            with open(f'storage_expand_statement/results/{idx+1}_{jdx+1}_successed.txt', 'w') as file:
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
            with open(f'storage_expand_statement/results/{idx+1}_{jdx+1}_failed.txt', 'w') as file:
                file.write(file_buffer)
    
    print("[Done] Expand statement.")

    with open('storage_expand_statement/table_document_set.json', 'w') as file:
        json.dump(table_document_set, file, indent=4)

    ### BUFFER ###
    with open('storage_expand_statement/buffer.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    ### BUFFER ###

    print(f"Cost: ${cost:.2f}")

    print(f"Success: {success_cnt}  |   Fail: {fail_cnt}")
