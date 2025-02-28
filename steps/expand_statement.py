import asyncio
import json
import pandas as pd
import re
import traceback
from collections import defaultdict
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Literal, Tuple


async def expand_statement_task(
        model_input: List[Dict[str, Any]],
        model_name: str,
        classification: Literal['high_header_sim', 'low_header_sim'],
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: expand statement with high header sim

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
                system_prompt=load_prompt(role='system', task='expand_statement_with_high_header_sim'),
                user_prompt=load_prompt(role='user', task='expand_statement_with_high_header_sim').format(
                    shots=input_data['shots'],
                    dataframe_schema=f"DataFrame [caption] {input_data['df_caption']} " + \
                        f"[columns] {' | '.join(input_data['df_columns'])} " + \
                        f"[first row] {' | '.join(input_data['df_first_row'])}",
                    sql_query=input_data['sql_query'],
                    statement=input_data['statement']
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
                system_prompt=load_prompt(role='system', task='expand_statement_with_high_header_sim'),
                user_prompt=load_prompt(role='user', task='expand_statement_with_high_header_sim').format(
                    shots=input_data['shots'],
                    dataframe_schema=f"DataFrame [caption] {input_data['df_caption']} " + \
                        f"[columns] {' | '.join(input_data['df_columns'])} " + \
                        f"[first row] {' | '.join(input_data['df_first_row'])}",
                    sql_query=input_data['sql_query'],
                    statement=input_data['statement']
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


def expand_statement(
        table_lake: Dict[str, Dict[str, Any]],
        instance_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        load_shot: object,
        model_name: str,
        batch_size: int
    ):
    """Task: expand statement

    [Params]
    table_lake     : Dict[str, Dict[str, Any]]
    instance_set   : List[Dict[str, Any]]
    classification : Literal['high_header_sim', 'low_header_sim']
    load_shot      : object
    model_name     : str
    batch_size     : int

    [Returns]
    table_document_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initalization and setup
    statement_pattern_set = defaultdict(list) # Output 1
    nl_document_list = defaultdict(list) # Output 2

    for role in ['system', 'user']:
        task = f'expand_statement_with_{classification}'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)
    
    if classification == 'low_header_sim':
        with open('additional_process_source_spidertableqa_dataset/storage/joined_table_set.json', 'r') as file:
                joined_table_set = json.load(file)

    # Main task
    appended = set()

    model_input = []
    for idx, instance in enumerate(instance_set):
        gold_table_set = [table_lake[t_id] for t_id in instance['gold_table_id_set']]
        data_list = instance['data_list']

        ###
        if classification == 'high_header_sim':
            for jdx, data in enumerate(data_list):
                silver_table = next(tb for tb in gold_table_set if tb['id'] in data['entailed_table_id_set'])

                if (silver_table['id'], data['statement']) in appended:
                    continue

                appended.add((silver_table['id'], data['statement'])) # remove duplicate

                model_input.append({
                    'df_caption': silver_table['metadata'],
                    'df_columns': silver_table['header'],
                    'df_first_row': silver_table['cell'][0],
                    'sql_query': data['sql_query'],
                    'statement': data['statement'],
                    'key': (idx, jdx),
                    'shots': load_shot()
                })
        
        elif classification == 'low_header_sim':
            joined_table = next(tb for tb in joined_table_set if tb['table_id_set'] == instance['gold_table_id_set'])
            
            db_name = gold_table_set[0]['metadata'].split('|')[0].strip()
            table_name_set = [table['metadata'].split('|')[1].strip() for table in gold_table_set]
            joined_table_metadata = f"{db_name} | {', '.join(table_name_set)}"

            for jdx, data in enumerate(data_list):
                model_input.append({
                    'df_caption': joined_table_metadata,
                    'df_columns': joined_table['header'],
                    'df_first_row': joined_table['cell'][0] if joined_table['cell'] != [] else [],
                    'sql_query': data['sql_query'],
                    'statement': data['statement'],
                    'key': (idx, jdx),
                    'shots': load_shot()
                })
        ###

    semaphore = asyncio.Semaphore(batch_size)   
    task_output_list, cost = asyncio.run(expand_statement_task(
        model_input=model_input,
        classification=classification,
        model_name=model_name,
        semaphore=semaphore
    ))

    clear_storage(storage_path=f"buffer/{classification}/expand_statement", extension='txt')

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx = task_output['key']

        ###
        if classification == 'high_header_sim':
            table_id = next(t_id for t_id in instance_set[idx]['data_list'][jdx]['entailed_table_id_set'])
            table = table_lake[table_id]

            nl_document_list[table_id].append(instance_set[idx]['data_list'][jdx]['statement']) # given statement
        
        elif classification == 'low_header_sim':
            table_id_set = tuple(instance_set[idx]['gold_table_id_set'])

            nl_document_list[table_id_set].append(
                instance_set[idx]['data_list'][jdx]['statement']
            ) # given statement
        ###

        try:
            import num2words
            from itertools import permutations, combinations

            ###
            if classification == 'high_header_sim':
                local_vars = {'df': pd.DataFrame(data=table['cell'], columns=table['header'])}
                code = re.search(r"```python\s+([\s\S]+?)```", task_output['response']).group(1)
                exec(f"{code}\nstatement_pattern, expanded_statement_list = expand_statement_pattern(df=df)", globals(), local_vars)
            
            elif classification == 'low_header_sim':
                joined_table = next(tb for tb in joined_table_set if tuple(tb['table_id_set']) == table_id_set)

                local_vars = {'df': pd.DataFrame(data=joined_table['cell'], columns=joined_table['header'])}
                code = re.search(r"```python\s+([\s\S]+?)```", task_output['response']).group(1)
                exec(f"{code}\nstatement_pattern, expanded_statement_list = expand_statement_pattern(df=df)", globals(), local_vars)
            ###

            statement_pattern = local_vars['statement_pattern']
            expanded_statement_list = local_vars['expanded_statement_list']

            ###
            if classification == 'high_header_sim':
                statement_pattern_set[table_id].append(statement_pattern)
                nl_document_list[table_id].extend(expanded_statement_list)
            
            elif classification == 'low_header_sim':
                statement_pattern_set[table_id_set].append(statement_pattern)
                nl_document_list[table_id_set].extend(expanded_statement_list)
            ###
            
            success_cnt += 1
        
        except:
            with open(f'buffer/{classification}/expand_statement/{idx+1}_{jdx+1}_error.txt', 'w') as file:
                file.write(traceback.format_exc())

            fail_cnt += 1

    # Buffer
    with open(f'buffer/{classification}/expand_statement.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    
    ###
    if classification == 'high_header_sim':
        table_document_set = [
            {
                'table_id': table_id,
                'statement_pattern_set': list(set(statement_pattern_set[table_id])), # remain unique patterns
                'nl_document_list': list(set(nl_document_list[table_id])) # remain unique statements
            }
            for table_id in nl_document_list.keys()
        ]

    elif classification == 'low_header_sim':
        table_document_set = [
            {
                'table_id_set': list(table_id_set),
                'statement_pattern_set': list(set(statement_pattern_set[table_id_set])), # remain unique patterns
                'nl_document_list': list(set(nl_document_list[table_id_set])) # remain unique statements
            }
            for table_id_set in nl_document_list.keys()
        ]
    ###

    return table_document_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.expand_statement':
    from utils.display import clear_storage
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
