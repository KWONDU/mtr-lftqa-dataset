import json
import os
import re
import sys
from tqdm.asyncio import tqdm_asyncio


async def generate_narrative_topic_set_from_encompassed_document(model_input, model_name, load_prompt, get_async_openai_response):
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='generate_fine_grained_topic_set_using_llm'),
            user_prompt=load_prompt(role='user', task='generate_fine_grained_topic_set_using_llm').format(
                table_header=' | '.join(input_data['table_header']),
                encompassed_document=input_data['encompassed_document']
            ),
            model_name=model_name
        ) for input_data in model_input
    ]

    model_output_list = await tqdm_asyncio.gather(*tasks)

    generate_narrative_topic_set_from_encompassed_document_task_output = {
        'system_prompt': [model_output['system_prompt'] for model_output in model_output_list],
        'user_prompt': [model_output['user_prompt'] for model_output in model_output_list],
        'response': [model_output['response'] for model_output in model_output_list],
    }

    generated_narrative_topic_set_list = [
        extract_fine_grained_topic_set_from_response(model_output['response'])
        for model_output in model_output_list
    ]

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return generate_narrative_topic_set_from_encompassed_document_task_output, generated_narrative_topic_set_list, cost


def extract_fine_grained_topic_set_from_response(response):
    None


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

    with open('source/generated_document_for_each_table.json', 'r') as file:
        encompassed_document_per_table = json.load(file)
    
    match_table_id_to_table_info = {table['id']: table for table in tabfact_tables}
    
    # Set prompt
    for role in ['system', 'user']:
        task = 'generate_fine_grained_topic_set_using_llm'
        save_prompt(f'prompt_construct_source_table2text_dataset/{role}/{task}.txt', role, task)
    
    # Input: each table_header and encompassed document
    # Output: narrative topic set
    model_name = 'gpt-3.5-turbo'
    generate_narrative_topic_set_from_encompassed_document_task_input = [
        {
            'table_header': match_table_id_to_table_info[table_id]['header'],
            'encompassed_document': encompassed_document
        }
        for table_id, encompassed_document in encompassed_document_per_table.items()
    ]

    _, generated_narrative_topic_set_list, cost = generate_narrative_topic_set_from_encompassed_document(
        model_input=generate_narrative_topic_set_from_encompassed_document_task_input,
        model_name=model_name,
        load_prompt=load_prompt,
        get_async_openai_response=get_async_openai_response
    )

    fine_grained_topic_set_per_table = {
        table['id']: fine_grained_topic_set
        for table, fine_grained_topic_set in zip(tabfact_tables, generated_narrative_topic_set_list)
    }
    
    # Save to file
    with open('storage/generated_fine_grained_topic_set_for_each_table.json', 'w') as file:
        json.dump(fine_grained_topic_set_per_table, file)

    print(f"Total cost: ${cost}")
