import argparse
import asyncio
import json
import logging
import random
import re
import time
from collections import defaultdict
from utils.dataset import load_source_dataset
from utils.openai import save_prompt


MODEL_NAME='gpt-3.5-turbo'

result_buffer_format = \
"""[Gold table set]
{display_table}

[Entailed statements]
{display_data}

[Document generation]
{generated_document}

[Answer annotation]
{annotated_answer}

[Question annotation]
{annotated_question}
"""


def main(dataset_name, sample_n):
     dataset = load_source_dataset(dataset_name=dataset_name)
     logger.info(dataset)

     source_type = dataset_name.replace('Source', '').lower()

     # Load tables and dataset
     table_lake = {table['id']: table for table in dataset.tables}
     data_dict = {tuple(data['gold_table_id_set']): data['data_list'] for data in dataset[:]}
     
     train_data_dict = {tuple(data['gold_table_id_set']): data['data_list'] for data in dataset.train} if dataset.train else dict()
     validation_data_dict = {tuple(data['gold_table_id_set']): data['data_list'] for data in dataset.validation} if dataset.validation else dict()
     test_data_dict = {tuple(data['gold_table_id_set']): data['data_list'] for data in dataset.test} if dataset.test else dict()

     """ # Dataset statistics
     print(f"# of unique tables: {len(table_lake)}")
     print(f"# of dataset: {len(data_dict)}")
     print(f"# of train set: {len(train_data_dict)}")
     print(f"# of validation set: {len(validation_data_dict)}")
     print(f"Avg. # of gold tables per data: {sum([len(_) for _ in data_dict.keys()]) / len(data_dict):.2f}")
     print(f"Avg. # of data per gold table set: {sum([len(_) for _ in data_dict.values()]) / len(data_dict):.2f}")
     """

     # Sample data
     random.seed(42) # fix sampled data
     sampled_data_dict = dict(random.sample(list(data_dict.items()), sample_n))

     # Initialization
     result_buffer = defaultdict(dict)
     llm_buffer = []
     total_cost = defaultdict()

     # 0. Display information of gold table set and data set
     from utils.display import table_visualization
     for num, (gold_table_id_set, data_list) in enumerate(sampled_data_dict.items()):
          display_table = "\n".join([
               table_visualization(
                    table_num=idx+1,
                    metadata=table_lake[gold_table_id]['metadata'],
                    header=table_lake[gold_table_id]['header'],
                    cell=table_lake[gold_table_id]['cell']
               ) for idx, gold_table_id in enumerate(gold_table_id_set)
          ])

          display_data = "\n".join([
               data
               for _, data in enumerate(data_list)
          ])

          result_buffer[num].update({
               'display_table': display_table,
               'display_data': display_data
          })

     logger.info("[Done] Display information of gold table set and data set.")

     ###

     # Dataset structure
     # type: SourceTable2Text, SourceText2SQL
     # tables: {table_id, metadata, header, cell}
     # split set: {gold_table_id_set, data_list}
     #                                data_list: list of entailed statements

     for role in ['system', 'user']:
          for task in ['generate_document', 'annotate_answer', 'annotate_question']:
               save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)
     
     # 1. Generate document using entailed statements and gold table set information
     from plans import generate_document

     step1_task_input = [
          {
               'gold_table_set': [
                    table_lake[gold_table_id]
                    for gold_table_id in gold_table_id_set
               ],
               'statement_list': data_list
          }
          for gold_table_id_set, data_list in sampled_data_dict.items()
     ]

     semaphore = asyncio.Semaphore(100)
     step1_task_output, cost = asyncio.run(generate_document(
          semaphore=semaphore,
          model_input=step1_task_input,
          model_name=MODEL_NAME
     ))

     generated_document_list = [model_output['response'] for model_output in step1_task_output]

     for num, generated_document in enumerate(generated_document_list):
          result_buffer[num].update({
               'doc': generated_document
          })
     
     ### BUFFER ###
     llm_buffer.append(step1_task_output)

     total_cost['Generate document'] = cost
     ### BUFFER ###

     logger.info("[Done] Generate document using entailed statements and gold table set information.")

     # 2. Annotate high-level answer using generated document
     from plans import annotate_answer

     step2_task_input = [
          {
               'document': generated_document
          }
          for generated_document in generated_document_list
     ]

     semaphore = asyncio.Semaphore(100)
     step2_task_output, cost = asyncio.run(annotate_answer(
          semaphore=semaphore,
          model_input=step2_task_input,
          model_name=MODEL_NAME
     ))

     annotated_answers_list = []
     for model_output in step2_task_output:
          annotated_answers_list.append(
               [line for line in re.sub(r'^\d+\.\s*', '', model_output['response'], flags=re.MULTILINE).splitlines() if line.strip()]
          )

     for num, annotated_answers in enumerate(annotated_answers_list):
          result_buffer[num]['annotation'] = {
               f'ans_{idx}': annot_ans
               for idx, annot_ans in enumerate(annotated_answers)
          }

     ### BUFFER ###
     llm_buffer.append(step2_task_output)
     
     logger.info("[Done] Annotate high-level answer using generated document.")
     ### BUFFER

     # 3. Annotate high-level question using annotated high-level answer and gold table set information
     from plans import annotate_question

     step3_task_input = [
          {
               'gold_table_set': [
                    table_lake[gold_table_id]
                    for gold_table_id in gold_table_id_set
               ],
               'answer': annotated_answer,
               'key': (num, idx)
          }
          for num, (gold_table_id_set, annotated_answers) in enumerate(zip(sampled_data_dict.keys(), annotated_answers_list))
          for idx, annotated_answer in enumerate(annotated_answers)
     ]

     semaphore = asyncio.Semaphore(100)
     step3_task_output, cost = asyncio.run(annotate_question(
          semaphore=semaphore,
          model_input=step3_task_input,
          model_name=MODEL_NAME
     ))

     annotated_questions_list = [[] for _ in range(len(annotated_answers_list))]
     for model_output in step3_task_output:
          annotated_questions_list[model_output['key'][0]].append(model_output['response'])

     for num, annotated_questions in enumerate(annotated_questions_list):
          result_buffer[num]['annotation'].update({
               f'qn_{idx}': annot_qn
               for idx, annot_qn in enumerate(annotated_questions)
          })

     ### BUFFER ###
     llm_buffer.append(step3_task_output)
     
     logger.info("[Done] Annotate high-level question using annotated high-level answer and gold table set information.")
     ### BUFFER

     # 4. Save to file - temp
     for num, result in result_buffer.items():
          for idx in range(len(result['annotation']) // 2):
               with open(f'results/storage/{source_type}_{num}_{idx}.txt', 'w') as file:
                    file.write(result_buffer_format.format(
                         display_table=result['display_table'],
                         display_data=result['display_data'],
                         generated_document=result['doc'],
                         annotated_answer=result['annotation'][f'ans_{idx}'],
                         annotated_question=result['annotation'][f'qn_{idx}']
                    ))

     with open(f'results/buffer/{source_type}_llm.json', 'w') as file:
          json.dump(llm_buffer, file, indent=4)
    
     logger.info(time.strftime("%Y/%m/%d %H:%M:%S", time.localtime()))
     for task, cost in total_cost.items():
          logger.info(f"{task}: ${cost:.2f}")


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
