import asyncio
import json
import os
import re
import sys
from tqdm.asyncio import tqdm_asyncio


async def generate_narrative_topic_set_from_encompassed_document(model_input, model_name, load_prompt, get_async_openai_response_with_semaphore):
    tasks = [
        get_async_openai_response_with_semaphore(
            system_prompt=load_prompt(role='system', task='generate_fine_grained_topic_set_using_llm'),
            user_prompt=load_prompt(role='user', task='generate_fine_grained_topic_set_using_llm').format(
                tabular_data=input_data['tabular_data'],
                encompassed_document=input_data['encompassed_document']
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


def extract_fine_grained_topic_set_from_response(response):
    narrative_topic_pattern = r"\d+\.\s*(.*)"

    narrative_topic_set = re.findall(narrative_topic_pattern, response)

    return [topic.strip() for topic in narrative_topic_set]


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

    with open('storage/generated_document_for_each_table.json', 'r') as file:
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
            'table_id': table_id,
            'tabular_data': table_format(
                table_num=0,
                metadata=None,
                header=match_table_id_to_table_info[table_id]['header'],
                cell=match_table_id_to_table_info[table_id]['cell'],
                serialize=True
            ),
            'encompassed_document': encompassed_document
        }
        for table_id, encompassed_document in encompassed_document_per_table.items()
    ]

    generate_narrative_topic_set_from_encompassed_document_task_input = [generate_narrative_topic_set_from_encompassed_document_task_input[0]]

    model_output_list, cost = asyncio.run(generate_narrative_topic_set_from_encompassed_document(
        model_input=generate_narrative_topic_set_from_encompassed_document_task_input,
        model_name=model_name,
        load_prompt=load_prompt,
        get_async_openai_response_with_semaphore=get_async_openai_response_with_semaphore
    ))

    fine_grained_topic_set_per_table = {
        model_output['key']: extract_fine_grained_topic_set_from_response(model_output['response'])
        for model_output in model_output_list
    }
    
    # Save to file
    with open('storage/generated_fine_grained_topic_set_for_each_table.json', 'w') as file:
        json.dump(fine_grained_topic_set_per_table, file, indent=4)

    ###

    with open('buffer/generate_fine_grained_topic_set_using_llm.json', 'w') as file:
        json.dump(model_output_list, file, indent=4)

    print(f"Total cost: ${cost:.2f}")
