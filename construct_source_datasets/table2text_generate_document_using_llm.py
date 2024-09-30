import asyncio
import json
import os
import sys
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio


async def generate_encompassed_document_from_entailed_statements(model_input, model_name, load_prompt, get_async_openai_response_with_semaphore):
    tasks = [
        get_async_openai_response_with_semaphore(
            system_prompt=load_prompt(role='system', task='generate_document_using_llm'),
            user_prompt=load_prompt(role='user', task='generate_document_using_llm').format(
                tabular_data=input_data['tabular_data'],
                entailed_statements='\n'.join([f'{i+1}. {entailed_statement}' for i, entailed_statement in enumerate(input_data['entailed_statements'])])
            ),
            model_name=model_name,
            key=input_data['table_id']
        ) for input_data in model_input
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return model_output_list, cost


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    from utils.format import table_format
    from utils.openai import get_async_openai_response_with_semaphore, load_prompt, save_prompt
    return load_dataset, table_format, get_async_openai_response_with_semaphore, load_prompt, save_prompt


if __name__ == '__main__':
    load_dataset, table_format, get_async_openai_response_with_semaphore, load_prompt, save_prompt = import_utils()

    tabfact = load_dataset(dataset_name='TabFact')
    tabfact_tables = tabfact.tables
    tabfact_train_set = tabfact.train
    tabfact_validation_set = tabfact.validation
    tabfact_test_set = tabfact.test

    # Extract entailed statements for each table
    match_table_id_to_statement_list = defaultdict(list)
    for data in tabfact_train_set + tabfact_validation_set + tabfact_test_set:
        match_table_id_to_statement_list[''.join(data['gold_tables'])].append(
            next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type)
        )
    
    # Set prompt
    for role in ['system', 'user']:
        task = 'generate_document_using_llm'
        save_prompt(f'prompt_construct_source_table2text_dataset/{role}/{task}.txt', role, task)
    
    # Input: each table_header and entailed_statements
    # Output: encompassed document
    model_name = 'gpt-3.5-turbo'
    generate_encompassed_document_from_entailed_statements_task_input = [
        {
            'table_id': table['id'],
            'table_header': table['header'],
            'entailed_statements': match_table_id_to_statement_list[table['id']]
        } for table in tabfact_tables if table['id']
    ]

    ###
    table = next(_ for _ in tabfact.tables if _['id'] == '9de550c00e38d6caaccd9f7815900f159499737aa03cc37b26cab05564701d55')
    generate_encompassed_document_from_entailed_statements_task_input = [
        {
            'table_id': table['id'],
            'tabular_data': table_format(table_num=0, metadata=None, header=table['header'], cell=table['cell'], serialize=True),
            'entailed_statements': [
                "the 2002 - 2003 season have 24 list as the number",
                "froilan baguion , number 17 have the guard position list for season 2008 and be acquire by free agency",
                "torraye bragg be number 50",
                "number 17 play in season 2008",
                "bryan basemore be number 8.sssss",
                "ronjay buenafe play the position of guard",
                "chris bolado , number 24 , be acquire via free agency during the 2002 - 2003 season",
                "froilan baguion be a guard who be acquire via free agency in 2008",
                "torraye bragg wear the jersey number 50 in 2002 season",
                "froilan baguion wear the jersey number 17 in 2008 season",
                "bryant basemore wear jersey number 8 in 2002 season",
                "ronjay buenafe play the guard position in the game from 2007 - 2009 season"
            ]
        }
    ]
    ###

    model_output_list, cost = asyncio.run(generate_encompassed_document_from_entailed_statements(
        model_input=generate_encompassed_document_from_entailed_statements_task_input,
        model_name=model_name,
        load_prompt=load_prompt,
        get_async_openai_response_with_semaphore=get_async_openai_response_with_semaphore
    ))

    generated_encompassed_document_per_table = {
        model_output['key']: model_output['response']
        for model_output in model_output_list
    }

    # Save to file
    with open('storage/generated_document_for_each_table.json', 'w') as file:
        json.dump(generated_encompassed_document_per_table, file, indent=4)
    
    ###

    with open('buffer/generate_document_using_llm.json', 'w') as file:
        json.dump(model_output_list, file, indent=4)

    print(f"Total cost: ${cost:.2f}")
