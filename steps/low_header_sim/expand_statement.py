import asyncio
import json
import pandas as pd
import re
import traceback
from collections import defaultdict
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Tuple


async def expand_statement_task(
        semaphore: asyncio.Semaphore,
        model_input: List[Dict[str, Any]],
        model_name: str
    ) -> Tuple[List[Dict[str, Any]], int]:
    """OpenAI response: expand statement

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
            system_prompt=load_prompt(role='system', task='expand_statement_with_low_header_sim'),
            user_prompt=load_prompt(role='user', task='expand_statement_with_low_header_sim').format(
                shots=input_data['shots'],
                df_schema_set="\n".join([
                    "\n".join([
                        f"\t- **DataFrame {tdx + 1}**:",
                        f"\t- **Caption**: {df_caption}",
                        f"\t- **Columns**: {' | '.join(df_columns)}",
                        f"\t- **First Row**: {' | '.join(df_first_row)}",
                        ""
                    ])
                    for tdx, (df_caption, df_columns, df_first_row) in enumerate(zip(input_data['df_caption_set'], input_data['df_columns_set'], input_data['df_first_row_set']))
                ]),
                sql_query=input_data['sql_query'],
                statement=input_data['statement']
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


def expand_statement(
        table_lake: Dict[str, Dict[str, Any]],
        instance_set: List[Dict[str, Any]],
        load_shot: object,
        model_name: str,
        semaphore: asyncio.Semaphore
    ):
    """Task: expand statement

    [Params]
    table_lake   : Dict[str, Dict[str, Any]]
    instance_set : List[Dict[str, Any]]
    load_shot    : object
    model_name   : str
    semaphore    : asyncio.Semaphore

    [Returns]
    table_document_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initalization and setup
    table_document_set = defaultdict(list) # Output

    for role in ['system', 'user']:
        task = 'expand_statement_with_low_header_sim'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(instance_set):
        gold_table_set = [table_lake[t_id] for t_id in sorted(instance['gold_table_id_set'])]
        data_list = instance['data_list']

        for jdx, data in enumerate(data_list):
            if len(data['entailed_table_id_set']) == 1:
                continue # only consider multi-table related data

            model_input.append({
                'df_caption_set': [tb['metadata'] for tb in gold_table_set],
                'df_columns_set': [tb['header'] for tb in gold_table_set],
                'df_first_row_set': [tb['cell'][0] for tb in gold_table_set],
                'sql_query': data['sql_query'],
                'statement': data['statement'],
                'key': (idx, jdx),
                'shots': load_shot()
            })
    
    task_output_list, cost = asyncio.run(expand_statement_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=model_name
    ))

    # Clear
    clear_storage(storage_path='buffer/low_header_sim/expand_statement', extension='txt')

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx = task_output['key']
        table_id_set = sorted(instance_set[idx]['gold_table_id_set'])

        table_document_set[tuple(table_id_set)].append(instance_set[idx]['data_list'][jdx]['statement']) # given statement
        file_buffer = "\n".join([
            "# Table information",
            "\n".join([
                table_visualization(
                    table_num=tdx + 1,
                    metadata=table_lake[t_id]['metadata'],
                    header=table_lake[t_id]['header'],
                    cell=table_lake[t_id]['cell']
                ) + "\n"
                for tdx, t_id in enumerate(table_id_set)
            ]),
            "# SQL query",
            instance_set[idx]['data_list'][jdx]['sql_query'],
            "",
            "# Statement",
            instance_set[idx]['data_list'][jdx]['statement'],
            ""
        ])

        try:
            local_vars = {
                f'df{tdx + 1}': pd.DataFrame(data=table_lake[t_id]['cell'], columns=table_lake[t_id]['header'])
                for tdx, t_id in enumerate(table_id_set)
            }
            code = re.search(r"```python\s+([\s\S]+?)```", task_output['response']).group(1)
            exec(
                f"{code}\nstatement_pattern, expanded_statement_list = expand_statement_pattern("
                + ", ".join([f"df{tdx + 1}=df{tdx + 1}" for tdx, _ in enumerate(table_id_set)])
                + ")"
                , globals(),
                local_vars
            )

            statement_pattern = local_vars['statement_pattern']
            expanded_statement_list = local_vars['expanded_statement_list']

            table_document_set[tuple(table_id_set)].extend(expanded_statement_list)
            file_buffer = "\n".join([
                file_buffer,
                "# Statement pattern",
                statement_pattern,
                "",
                "# Expanded staetments",
                "\n".join(expanded_statement_list),
                "",
                "# Python code",
                task_output['response'],
                ""
            ])

            success_cnt += 1
            with open(f'buffer/low_header_sim/expand_statement/{idx+1}_{jdx+1}_successed.txt', 'w') as file:
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
            with open(f'buffer/low_header_sim/expand_statement/{idx+1}_{jdx+1}_failed.txt', 'w') as file:
                file.write(file_buffer)

    # Buffer
    with open('buffer/low_header_sim/expand_statement.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return [
        {
            'table_id_set': table_id_set,
            'nl_document_list': list(set(nl_document_list)) # remain unique statements
        }
        for table_id_set, nl_document_list in table_document_set.items()
    ], success_cnt, fail_cnt, cost


if __name__ == 'steps.low_header_sim.expand_statement':
    from utils.display import clear_storage, table_visualization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
