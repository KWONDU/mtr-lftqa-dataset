import json
import os
import sys
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio


async def generate_encompassed_document_from_entailed_statements(model_input, model_name, get_async_openai_response):
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='generate_document_using_llm'),
            user_prompt=load_prompt(role='user', task='generate_document_using_llm').format(
                table_header=' | '.join(input_data['table_header']),
                entailed_statements='\n'.join([f'{i+1}. {entailed_statement}' for i, entailed_statement in enumerate(input_data['entailed_statements'])])
            ),
            model_name=model_name
        ) for input_data in model_input
    ]

    model_output_list = await tqdm_asyncio.gather(*tasks)

    generate_encompassed_document_from_entailed_statements_task_output = {
        'system_prompt': [model_output['system_prompt'] for model_output in model_output_list],
        'user_prompt': [model_output['user_prompt'] for model_output in model_output_list],
        'response': [model_output['response'] for model_output in model_output_list],
    }

    generated_encompasssed_document_list = [
        model_output['response']
        for model_output in model_output_list
    ]

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return generate_encompassed_document_from_entailed_statements_task_output, generated_encompasssed_document_list, cost


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
    return load_dataset, get_async_openai_response, load_prompt, save_prompt


if __name__ == '__main__':
    load_dataset, get_async_openai_response, load_prompt, save_prompt = import_utils()

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
            'table_header': table['header'],
            'entailed_statements': match_table_id_to_statement_list[table['id']]
        } for table in tabfact_tables if table['id']
    ]

    _, generated_encompassed_document_list, cost = generate_encompassed_document_from_entailed_statements(
        model_input=generate_encompassed_document_from_entailed_statements_task_input,
        model_name=model_name,
        load_prompt=load_prompt,
        get_async_openai_response=get_async_openai_response
    )

    generated_encompassed_document_per_table = {
        table['id']: encompassed_document
        for table, encompassed_document in zip(tabfact_tables, generated_encompassed_document_list)
    }

    # Save to file
    with open('storage/generated_document_for_each_table.json', 'w') as file:
        json.dump(generated_encompassed_document_per_table, file)
    
    print(f"Total cost: ${cost}")
