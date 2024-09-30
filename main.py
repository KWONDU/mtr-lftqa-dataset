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

result_buffer_format = \
"""[Gold table set information]
{display_table}

[Data set information]
{display_data}

[Document generation]
{generate_document}

[Answer annotation]
{annotate_answer}

[Question annotation]
{annotate_question}
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
     from utils.format import data_format, table_format
     for num, (gold_table_id_set, data_list) in enumerate(sampled_data_dict.items()):
          display_table = "\n\n".join([
               table_format(
                    table_num=idx+1,
                    metadata=table_lake[gold_table_id]['metadata'],
                    header=table_lake[gold_table_id]['header'],
                    cell=table_lake[gold_table_id]['cell'],
                    serialize=False
               ) for idx, gold_table_id in enumerate(gold_table_id_set)
          ])

          display_data = "\n\n".join([
               data_format(
                    data_num=idx+1,
                    data=data,
                    type='sentence'
               )
               for idx, data in enumerate(data_list)
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
     #                                data_list: list of entailed sentences

     for role in ['system', 'user']:
          for task in ['generate_document', 'annotate_high_level_question', 'annotate_high_level_answer']:
               save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)
     
     # 1. Generate document using entailed sentences and gold table set information
     from plans import generate_document

     step1_task_input = [
          {
               'gold_table_set': [
                    table_lake[gold_table_id]
                    for gold_table_id in gold_table_id_set
               ],
               'sentence_list': data_list
          }
          for gold_table_id_set, data_list in sampled_data_dict.items()
     ]

     semaphore=asyncio.Semaphore(100)
     step1_task_output, cost = asyncio.run(generate_document(
          semaphore=semaphore,
          model_input=step1_task_input,
          model_name=MODEL_NAME
     ))

     generated_document_list = [model_output['response'] for model_output in step1_task_output]

     for num, model_output in enumerate(step1_task_output):
          result_buffer[num].update({
               'generate_document': model_output['response']
          })
     
     ### BUFFER ###
     llm_buffer.append(step1_task_output)

     total_cost['Generate document'] = cost
     ### BUFFER ###

     logger.info("[Done] Generate document using entailed sentences and gold table set information.")

     # 2. Annotate high-level answer using generated document
     from plans import annotate_answer

     for _, __ in result_buffer.items():
          result_buffer[_].update({
               'annotate_answer': ""
          })
     
     step2_task_output = []

     ### BUFFER ###
     llm_buffer.append(step2_task_output)
     
     logger.info("[Done] Annotate high-level answer using generated document.")
     ### BUFFER

     # 3. Annotate high-level question using annotated high-level answer and gold table set information
     from plans import annotate_question

     for _, __ in result_buffer.items():
          result_buffer[_].update({
               'annotate_question': ""
          })
     
     step3_task_output = []

     ### BUFFER ###
     llm_buffer.append(step3_task_output)
     
     logger.info("[Done] Annotate high-level question using annotated high-level answer and gold table set information.")
     ### BUFFER

     # 4. Save to file - temp
     for num, result in result_buffer.items():
          with open(f'results/annotation/{source_type}_{num}.txt', 'w') as file:
               file.write(result_buffer_format.format(
                    display_table=result['display_table'],
                    display_data=result['display_data'],
                    generate_document=result['generate_document'],
                    annotate_answer=result['annotate_answer'],
                    annotate_question=result['annotate_question']
               ))

     with open(f'results/{source_type}_llm_buffer.json', 'w') as file:
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
