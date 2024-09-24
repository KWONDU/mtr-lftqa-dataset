import argparse
import asyncio
import json
import logging
import random
import time
from collections import defaultdict
from utils.dataset import load_source_dataset
from utils.openai import save_prompt


MODEL_NAME='gpt-3.5-turbo'
QA_SET_LEN = 5

result_buffer_format = \
{
     'text2sql': \
"""[Gold table set information]
{display_table}

[Data set information]
{display_data}

[Generated question]
{generated_question}

[Generated answer]
{generated_answer}
""",
     'table2text': \
"""[Gold table set information]
{display_table}

[Data set information]
{display_data}

[Generated answer]
{generated_answer}

[Generated question]
{generated_question}
"""
}


def main(dataset_name, sample_n):
     dataset = load_source_dataset(dataset_name=dataset_name)
     logger.info(dataset)

     source_type = dataset_name.replace('Source', '').lower()

     # Load tables and dataset
     table_lake = {table['id']: table for table in dataset.tables}
     data_dict = {tuple(data['gold_table_id_set']): data['data_list'] for data in dataset[:]}

     """ # Dataset statistics
     print(f"# of unique tables: {len(table_lake)}")
     print(f"# of dataset: {len(data_dict)}")
     print(f"Avg. # of gold tables per data: {sum([len(_) for _ in data_dict.keys()]) / len(data_dict):.2f}")
     print(f"Avg. # of data per gold table set: {sum([len(_) for _ in data_dict.values()]) / len(data_dict):.2f}")
     """

     # Save prompts
     for role in ['system', 'user']:
          for task in ['generate_high_level_questions', 'generate_high_level_answer']:
               save_prompt(f'prompt_{source_type}/{role}/{task}.txt', role, task)

     # Sample data
     random.seed(42) # fix sampled data
     sampled_data_dict = dict(random.sample(list(data_dict.items()), sample_n))

     # Initialization
     result_buffer = defaultdict(list)
     llm_buffer = []
     total_cost = defaultdict()

     # 0. Display information of gold table set and data set
     from plans.step0 import display_info
     for idx, (gold_table_id_set, data_list) in enumerate(sampled_data_dict.items()):
          display_table = display_info(
               type='table',
               data=[table_lake[gold_table_id] for gold_table_id in gold_table_id_set]
          )

          display_data = display_info(
               type='data',
               data=data_list
          )

          for num in range(idx * QA_SET_LEN, (idx + 1) * QA_SET_LEN):
               result_buffer[num].append(
                    {
                         'gold_table_set': display_table,
                         'data_list': display_data
                    }
               )

     logger.info("[Done] Display information of gold table set and data set.")

     ###

     # SourceText2SQL
     if source_type == 'text2sql':
          from plans.step1_text2sql import generate_high_level_questions
          from plans.step2_text2sql import generate_high_level_answer

          # 1. Generate high-level questions using gold table metadata/header set and corresponding question-SQL query pairs
          generate_high_level_questions_task_input = [
               {
                    'gold_table_set': [table_lake[gold_table_id] for gold_table_id in gold_table_id_set],
                    'data_list': data_list
               } for gold_table_id_set, data_list in sampled_data_dict.items()
          ]

          generate_high_level_questions_task_output, generated_questions_list, cost = asyncio.run(generate_high_level_questions(
               model_input=generate_high_level_questions_task_input,
               model_name=MODEL_NAME
          ))

          for idx, generated_questions in enumerate(generated_questions_list):
               for jdx, generated_question in enumerate(generated_questions):
                    result_buffer[idx * QA_SET_LEN + jdx].append(generated_question)

          ### BUFFER ###
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
          ### BUFFER ###

          logger.info("[Done] Generate high-level questions using gold table metadata/header set and corresponding question-SQL query pairs.")

          ###

          generated_each_question_list = [__ for _ in generated_questions_list for __ in _]

          ###

          # 2. Generate high-level answer using generated question and gold full-table set
          generate_high_level_answer_task_input = [
               {
                    'high_level_question': generated_question,
                    'tables_info': [
                         {'metadata': table_lake[gold_table_id]['metadata'], 'header': table_lake[gold_table_id]['header'], 'cell': table_lake[gold_table_id]['cell']}
                         for gold_table_id in gold_table_id_set
                    ]
               }
               for generated_question in generated_each_question_list
          ]

          generate_high_level_answer_task_output, generated_answer_list, cost = asyncio.run(generate_high_level_answer(
               model_input=generate_high_level_answer_task_input,
               model_name=MODEL_NAME
          ))

          for num, generated_answer in enumerate(generated_answer_list):
               result_buffer[num].append(generated_answer)

          ### BUFFER ###
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
          ### BUFFER ###

          logger.info("[Done] Generate high-level answer using generated question and gold full-table set.")

          ###

          for num, result in result_buffer.items():
               idx, jdx = num // QA_SET_LEN, num % QA_SET_LEN
               with open(f'results/annotation/{source_type}-{idx}-{jdx}.txt', 'w') as file:
                    file.write(result_buffer_format[source_type].format(
                         display_table=result[0]['gold_table_set'],
                         display_data=result[0]['data_list'],
                         generated_question=result[1],
                         generated_answer=result[2]
                    ))
     
     # SourceTable2Text
     elif source_type == 'table2text':
          from plans.step1_table2text import generate_high_level_answers
          from plans.step2_table2text import generate_high_level_question

          # 1. Generate high-level answers using gold table metadata/header set and entailed statements
          generate_high_level_answers_task_input = [
               {
                    'gold_table_set': [table_lake[gold_table_id] for gold_table_id in gold_table_id_set],
                    'data_list': data_list
               } for gold_table_id_set, data_list in sampled_data_dict.items()
          ]

          generate_high_level_answers_task_output, generated_answers_list, cost = asyncio.run(generate_high_level_answers(
               model_input=generate_high_level_answers_task_input,
               model_name=MODEL_NAME
          ))

          for idx, generated_answers in enumerate(generated_answers_list):
               for jdx, generated_answer in enumerate(generated_answers):
                    result_buffer[idx * QA_SET_LEN + jdx].append(generated_answer)

          ### BUFFER ###
          for idx, (system_prompt, user_prompt, response) in enumerate(zip(generate_high_level_answers_task_output['system_prompt'],
                                                                           generate_high_level_answers_task_output['user_prompt'],
                                                                           generate_high_level_answers_task_output['response']
                                                                           )):
               for num in range(idx * QA_SET_LEN, (idx + 1) * QA_SET_LEN):
                    llm_buffer.append(
                         {
                              'num': num,
                              'task': 'generate high-level answers',
                              'system_prompt': system_prompt,
                              'user_prompt': user_prompt,
                              'response': response
                         }
                    )

          total_cost['Generate high-level answers'] = cost
          ### BUFFER ###

          logger.info("[Done] Generate high-level answers using gold table metadata/header set and entailed statements.")

          ###

          generated_each_answer_list = [__ for _ in generated_answers_list for __ in _]

          ###

          # 2. Generate high-level question using generated answer and gold full-table set
          generate_high_level_question_task_input = [
               {
                    'high_level_answer': generated_answer,
                    'tables_info': [
                         {'metadata': table_lake[gold_table_id]['metadata'], 'header': table_lake[gold_table_id]['header'], 'cell': table_lake[gold_table_id]['cell']}
                         for gold_table_id in gold_table_id_set
                    ]
               }
               for generated_answer in generated_each_answer_list
          ]

          generate_high_level_question_task_output, generated_question_list, cost = asyncio.run(generate_high_level_question(
               model_input=generate_high_level_question_task_input,
               model_name=MODEL_NAME
          ))

          for num, generated_question in enumerate(generated_question_list):
               result_buffer[num].append(generated_question)

          ### BUFFER ###
          for num, (system_prompt, user_prompt, response) in enumerate(zip(generate_high_level_question_task_output['system_prompt'],
                                                                           generate_high_level_question_task_output['user_prompt'],
                                                                           generate_high_level_question_task_output['response']
                                                                           )):
               llm_buffer.append(
                    {
                         'num': num,
                         'task': 'generate high-level question',
                         'system_prompt': system_prompt,
                         'user_prompt': user_prompt,
                         'response': response
                    }
               )

          total_cost['Generate high-level question'] = cost
          ### BUFFER ###

          logger.info("[Done] Generate high-level question using generated answer and gold full-table set.")

          ###

          for num, result in result_buffer.items():
               idx, jdx = num // QA_SET_LEN, num % QA_SET_LEN
               with open(f'results/annotation/{source_type}-{idx}-{jdx}.txt', 'w') as file:
                    file.write(result_buffer_format[source_type].format(
                         display_table=result[0]['gold_table_set'],
                         display_data=result[0]['data_list'],
                         generated_question=result[1],
                         generated_answer=result[2]
                    ))
     
     else:
          exit()

     ###

     with open(f'results/{source_type}-llm.json', 'w') as file:
          json.dump(llm_buffer, file)
    
     logger.info(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))
     for task, costs in total_cost.items():
          logger.info(f"{task}: ${costs:.2f}")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, required=True, choices=['SourceText2SQL', 'SourceTable2Text'], help='dataset name')
    parser.add_argument('-n', type=int, required=True, help='number of sampled data')

    args, _ = parser.parse_known_args()
    logger.info(args)

    """
    api_key = '___YOUR_OWN_OPENAI_API_KEY___'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """

    main(
        dataset_name=args.d,
        sample_n=args.n
    )
