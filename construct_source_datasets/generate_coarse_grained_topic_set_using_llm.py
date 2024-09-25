import json
import os
import re
import sys
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio


async def generate_coarse_grained_topic_set_from_category_set(model_input, model_name, load_prompt, get_async_openai_response):
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='generate_coarse_grained_topic_set_using_llm'),
            user_prompt=load_prompt(role='user', task='generate_coarse_grained_topic_set_using_llm').format(
                page_title=input_data['page_title'],
                category_set=", ".join([category for category in input_data['category_set']])
            ),
            model_name=model_name
        ) for input_data in model_input
    ]

    model_output_list = await tqdm_asyncio.gather(*tasks)

    generate_coarse_grained_topic_set_from_category_set_task_output = {
        'system_prompt': [model_output['system_prompt'] for model_output in model_output_list],
        'user_prompt': [model_output['user_prompt'] for model_output in model_output_list],
        'response': [model_output['response'] for model_output in model_output_list],
    }

    generated_coarse_grained_topic_set_list = [
        extract_coarse_grained_topic_set_from_response(model_output['response'])
        for model_output in model_output_list
    ]

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return generate_coarse_grained_topic_set_from_category_set_task_output, generated_coarse_grained_topic_set_list, cost


def extract_coarse_grained_topic_set_from_response(response):
    high_level_pattern = re.compile(r"High-level topic set:\n\s*-\s*(.*?)(?=\n\n|Middle-level topic set:)", re.DOTALL)
    middle_level_pattern = re.compile(r"Middle-level topic set:\n\s*-\s*(.*?)(?=\n\n|Low-level topic set:)", re.DOTALL)
    low_level_pattern = re.compile(r"Low-level topic set:\n\s*-\s*(.*)", re.DOTALL)

    high_level_match = high_level_pattern.search(response)
    middle_level_match = middle_level_pattern.search(response)
    low_level_match = low_level_pattern.search(response)

    high_level_topic_set = high_level_match.group(1).split("\n-") if high_level_match else []
    middle_level_topic_set = middle_level_match.group(1).split("\n-") if middle_level_match else []
    low_level_topic_set = low_level_match.group(1).split("\n-") if low_level_match else []

    return {
        "high_level": [topic.strip() for topic in high_level_topic_set],
        "middle_level": [topic.strip() for topic in middle_level_topic_set],
        "low_level": [topic.strip() for topic in low_level_topic_set]
    }


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

    with open('source/wikipedia_category_set_for_each_table.json', 'r') as file:
        wikipedia_category_set_for_each_table_dict = json.load(file)
    
    # Dict(page_title: category_set)
    category_set_per_page = defaultdict(set)
    for table_id, category_set in wikipedia_category_set_for_each_table_dict.items():
        each_table = next(table for table in tabfact_tables if table['id'] == table_id)
        page_title = each_table['metadata']
        category_set_per_page[page_title].update(category_set)
    
    # Set prompt
    for role in ['system', 'user']:
        task = 'generate_coarse_grained_topic_set_using_llm'
        save_prompt(f'prompt_construct_source_table2text_dataset/{role}/{task}.txt', role, task)
    
    sorted_dict = dict(sorted(category_set_per_page.items(), key=lambda _: len(_[1])))
    reverse_sorted_dict = dict(sorted(category_set_per_page.items(), key=lambda _: len(_[1]), reverse=True))
    cnt = 0
    for k, v in sorted_dict.items():
        print(load_prompt(role='user', task='generate_coarse_grained_topic_set_using_llm').format(page_title=k, category_set=", ".join(_ for _ in v)))
        cnt += 1
        if cnt == 10:
            break
    cnt = 0
    for k, v in reverse_sorted_dict.items():
        print(load_prompt(role='user', task='generate_coarse_grained_topic_set_using_llm').format(page_title=k, category_set=", ".join(_ for _ in v)))
        cnt += 1
        if cnt == 10:
            break
    exit()
    
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

    _, generated_coarse_grained_topic_set_list, cost = generate_coarse_grained_topic_set_from_category_set(
        model_input=generate_coarse_grained_topic_set_from_category_set_task_input,
        model_name=model_name,
        load_prompt=load_prompt,
        get_async_openai_response=get_async_openai_response
    )

    coarse_grained_topic_set_per_page = defaultdict()
    for page_title, generated_coarse_grained_topic_set in zip(category_set_per_page.keys(), generated_coarse_grained_topic_set_list):
        coarse_grained_topic_set_per_page[page_title] = {
            'high_level': generated_coarse_grained_topic_set['high_level'],
            'middle_level': generated_coarse_grained_topic_set['middle_level'],
            'low_level': generated_coarse_grained_topic_set['low_level']
        }
    
    # Save to file
    with open('storage/generated_coarse_grained_topic_set_for_each_page.json', 'w') as file:
        json.dump(coarse_grained_topic_set_per_page, file)

    print(f"Total cost: ${cost}")
