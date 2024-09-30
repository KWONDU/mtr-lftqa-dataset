import asyncio
import json
import os
import re
import sys
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio


async def generate_coarse_grained_topic_set_from_category_set(model_input, model_name, load_prompt, get_async_openai_response_with_semaphore):
    tasks = [
        get_async_openai_response_with_semaphore(
            system_prompt=load_prompt(role='system', task='generate_coarse_grained_topic_set_using_llm'),
            user_prompt=load_prompt(role='user', task='generate_coarse_grained_topic_set_using_llm').format(
                page_title=input_data['page_title'],
                category_set=", ".join([category for category in input_data['category_set']])
            ),
            model_name=model_name,
            key=input_data['page_title']
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


def extract_coarse_grained_topic_set_from_response(response):
    high_level_pattern = r"High-level topic set:\s*\[(.*?)\]"
    middle_level_pattern = r"Middle-level topic set:\s*\[(.*?)\]"
    low_level_pattern = r"Low-level topic set:\s*\[(.*?)\]"

    high_level_match = re.search(high_level_pattern, response, re.DOTALL)
    middle_level_match = re.search(middle_level_pattern, response, re.DOTALL)
    low_level_match = re.search(low_level_pattern, response, re.DOTALL)

    high_level_topic_set = high_level_match.group(1).split(",") if high_level_match else []
    middle_level_topic_set = middle_level_match.group(1).split(",") if middle_level_match else []
    low_level_topic_set = low_level_match.group(1).split(",") if low_level_match else []

    return {
        'high_level': [topic.strip() for topic in high_level_topic_set if topic.strip()],
        'middle_level': [topic.strip() for topic in middle_level_topic_set if topic.strip()],
        'low_level': [topic.strip() for topic in low_level_topic_set if topic.strip()]
    }


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    from utils.openai import get_async_openai_response_with_semaphore, load_prompt, save_prompt
    return load_dataset, get_async_openai_response_with_semaphore, load_prompt, save_prompt


if __name__ == '__main__':
    load_dataset, get_async_openai_response_with_semaphore, load_prompt, save_prompt = import_utils()

    tabfact = load_dataset(dataset_name='TabFact')
    tabfact_tables = tabfact.tables

    with open('storage/wikipedia_category_set_for_each_page.json', 'r') as file:
        category_set_per_page = json.load(file)
    
    # Set prompt
    for role in ['system', 'user']:
        task = 'generate_coarse_grained_topic_set_using_llm'
        save_prompt(f'prompt_construct_source_table2text_dataset/{role}/{task}.txt', role, task)
    
    # Input: each page_title and category_set
    # Output: high-level topic set, middle-level topic set, low-level topic set
    model_name = 'gpt-3.5-turbo'
    generate_coarse_grained_topic_set_from_category_set_task_input = [
        {
            'page_title': page_title,
            'category_set': category_set
        }
        for page_title, category_set in category_set_per_page.items()
    ]

    model_output_list, cost = asyncio.run(generate_coarse_grained_topic_set_from_category_set(
        model_input=generate_coarse_grained_topic_set_from_category_set_task_input,
        model_name=model_name,
        load_prompt=load_prompt,
        get_async_openai_response_with_semaphore=get_async_openai_response_with_semaphore
    ))

    coarse_grained_topic_set_per_page = {
        model_output['key']: extract_coarse_grained_topic_set_from_response(model_output['response'])
        for model_output in model_output_list
    }
    
    # Save to file
    with open('storage/generated_coarse_grained_topic_set_for_each_page.json', 'w') as file:
        json.dump(coarse_grained_topic_set_per_page, file, indent=4)
    
    ###

    with open('buffer/generate_coarse_grained_topic_set_using_llm.json', 'w') as file:
        json.dump(model_output_list, file, indent=4)

    print(f"Total cost: ${cost:.2f}")
