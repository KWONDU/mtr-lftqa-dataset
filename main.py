import argparse
import asyncio
import json
import logging
import random
from collections import defaultdict
from utils.dataset import load_dataset
from utils.openai import save_prompt

from plans.step0 import display_info
from plans.step1 import generate_high_level_questions
from plans.step2 import verify_and_modify_each_generated_question
from plans.step3 import generate_high_level_answer


MODEL_NAME='gpt-3.5-turbo'
QA_SET_LEN = 5

result_buffer_format = \
"""[Gold table set information]
{display_table}

[Data set information]
{display_data}

[Generated question]
{generated_question}

[Verification / Modified question]
[{verification}] {modified_question}

[Generated answer]
{generated_answer}
"""


def extract_multi_table_data_pairs(table_lake, dataset):
    # 1. Transform table lake to make it easier to use: table_dict
    table_dict = {table['id']: table for table in table_lake}

    # 2. Group by same gold table set (data): data_dict
    data_dict = defaultdict(list)
    for data in dataset:
        data_dict[tuple(data['gold_tables'])].append(data)

    # 3. Remove pair with single table set or single data
    keys_to_delete = []
    for table_key, data_value in data_dict.items():
        if len(table_key) == 1 or len(data_value) == 1:
            keys_to_delete.append(table_key)
    for table_key in keys_to_delete:
        del data_dict[table_key]

    return table_dict, data_dict


def main(dataset_name, sample_n):
    dataset = load_dataset(dataset_name=dataset_name)
    logger.info(dataset)

    # Extract Spider sub-dataset
    table_lake = [table for table in dataset.tables if table['source'] == 'Spider']
    spider_subset = [data for data in dataset[:]
                     if all(gold_table_id in {table['id'] for table in table_lake}
                            for gold_table_id in data['gold_tables'])
                    ]

    # Extract gold multi-table set and corresponding multi data pairs
    table_dict, data_dict = extract_multi_table_data_pairs(table_lake=table_lake, dataset=spider_subset)

    # save prompt
    for role in ['system', 'user']:
        for task in ['generate_high_level_questions', 'verify_and_modify_each_generated_question', 'generate_high_level_answer']:
            save_prompt(f'prompt/{role}/{task}.txt', role, task)

    # Sample data
    random.seed(42) # fix sampled data
    sampled_data_dict = dict(random.sample(list(data_dict.items()), sample_n))

    # Initialization
    result_buffer = defaultdict(list)
    llm_buffer = []
    total_cost = defaultdict()

    # 0. Display information of gold table set and data set
    for idx, (gold_table_id_set, data_list) in enumerate(sampled_data_dict.items()):
        display_table = display_info(
             type='table',
             data=[table_dict[gold_table_id] for gold_table_id in gold_table_id_set]
        )

        display_data = display_info(
             type='data',
             data=data_list
        )

        for num in range(idx * QA_SET_LEN, (idx + 1) * QA_SET_LEN):
            result_buffer[num].append(
                 {
                      'gold_table_set': display_table,
                      'data_set': display_data
                 }
            )
    
    logger.info("[Done] Display information of gold table set and data set.")

    ###

    # 1. Generate high-level questions using corresponding question-SQL query pairs
    generate_high_level_questions_task_input = [
        [
            {
                'question': data['question'],
                'sql': next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type=='SQL'),
                'sub_table': next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type=='table')
            }
            for data in data_list
        ] for _, data_list in sampled_data_dict.items()
    ]

    generate_high_level_questions_task_output, generated_questions_list, cost = asyncio.run(generate_high_level_questions(
        model_input=generate_high_level_questions_task_input,
        model_name=MODEL_NAME
    ))

    for idx, generated_questions in enumerate(generated_questions_list):
         for jdx, generated_question in enumerate(generated_questions):
             result_buffer[idx * QA_SET_LEN + jdx].append(generated_question)

    for idx, (system_prompt, user_prompt, response) in enumerate(zip(generate_high_level_questions_task_output['system_prompt'],
                                                                     generate_high_level_questions_task_output['user_prompt'],
                                                                     generate_high_level_questions_task_output['response']
                                                                     )):
        for num in range(idx * QA_SET_LEN, (idx + 1) * QA_SET_LEN):
            llm_buffer.append(
                 {
                      'num': num,
                      'task': 'generate high-level questions',
                      'system_prompt': system_prompt,
                      'user_prompt': user_prompt,
                      'response': response
                 }
            )

    total_cost['Generate high-level questions'] = cost

    logger.info("[Done] Generate high-level questions using question-SQL query pairs.")

    ###

    # 2. Verify and modify each generated question using gold table metadata/header set
    verify_and_modify_each_generated_question_task_input = [
        {
            'generated_question': generated_question,
            'tables_info': [
                {'metadata': table_dict[gold_table_id]['metadata'], 'header': table_dict[gold_table_id]['header']}
                for gold_table_id in gold_table_id_set
            ]
        }
        for generated_questions in generated_questions_list
        for generated_question in generated_questions
    ]

    verify_and_modify_each_generated_question_task_output, verification_list, modified_each_question_list, cost = asyncio.run(verify_and_modify_each_generated_question(
        model_input=verify_and_modify_each_generated_question_task_input,
        model_name=MODEL_NAME
    ))

    for num, (verification, modified_question) in enumerate(zip(verification_list, modified_each_question_list)):
         result_buffer[num].append((verification, modified_question))

    for num, (system_prompt, user_prompt, response) in enumerate(zip(verify_and_modify_each_generated_question_task_output['system_prompt'],
                                                                     verify_and_modify_each_generated_question_task_output['user_prompt'],
                                                                     verify_and_modify_each_generated_question_task_output['response']
                                                                     )):
         llm_buffer.append(
              {
                   'num': num,
                   'task': 'verify and modify each generated question',
                   'system_prompt': system_prompt,
                   'user_prompt': user_prompt,
                   'response': response
              }
         )

    total_cost['Verify and modify each generated question'] = cost
    
    logger.info("[Done] Verify and modify each generated question using gold table metadata/header set.")

    ###

    # 3. Generate high-level answer using modified question and gold full-table set
    generate_high_level_answer_task_input = [
        {
            'high_level_question': modified_question,
            'tables_info': [
                {'metadata': table_dict[gold_table_id]['metadata'], 'header': table_dict[gold_table_id]['header'], 'cell': table_dict[gold_table_id]['cell']}
                for gold_table_id in gold_table_id_set
            ]
        }
        for modified_question in modified_each_question_list
    ]

    generate_high_level_answer_task_output, generated_answer_list, cost = asyncio.run(generate_high_level_answer(
        model_input=generate_high_level_answer_task_input,
        model_name=MODEL_NAME
    ))

    for num, generated_answer in enumerate(generated_answer_list):
         result_buffer[num].append(generated_answer)

    for num, (system_prompt, user_prompt, response) in enumerate(zip(generate_high_level_answer_task_output['system_prompt'],
                                                                     generate_high_level_answer_task_output['user_prompt'],
                                                                     generate_high_level_answer_task_output['response']
                                                                     )):
         llm_buffer.append(
              {
                   'num': num,
                   'task': 'generate high-level answer',
                   'system_prompt': system_prompt,
                   'user_prompt': user_prompt,
                   'response': response
              }
         )

    total_cost['Generate high-level answer'] = cost

    logger.info("[Done] Generate high-level answer using modified question and gold full-table set.")

    ###

    for num, result in result_buffer.items():
         idx, jdx = num // QA_SET_LEN, num % QA_SET_LEN
         with open(f'results/annotation/{idx}-{jdx}.txt', 'w') as file:
              file.write(result_buffer_format.format(
                   display_table=result[0]['gold_table_set'],
                   display_data=result[0]['data_set'],
                   generated_question=result[1],
                   verification=result[2][0],
                   modified_question=result[2][1],
                   generated_answer=result[3]
              ))

    with open(f'results/llm.json', 'w') as file:
         json.dump(llm_buffer, file)
    
    for task, costs in total_cost.items():
        logger.info(f"{task}: ${costs:.2f}")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default='MultiTabQA', help='dataset name')
    parser.add_argument('-n', type=int, default=1, help='number of sampled data')

    args, _ = parser.parse_known_args()
    logger.info(args)

    """
    api_key = 'your_api_key'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """

    main(
        dataset_name = args.d,
        sample_n = args.n
    )
