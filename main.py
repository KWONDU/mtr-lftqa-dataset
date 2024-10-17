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
"""# Gold table set information
{display_table}

# NL query list
{display_data}

# Step 1. question annotation
Question: {annotated_question}
Difficulty: {question_difficulty}
Reference: {question_reference}

[Step 2. answer annotation]
{annotated_answer}

[Step 3. verification]
{verification}
"""


def run_plan(plan, plan_input):
     semaphore = asyncio.Semaphore(100)
     plan_output_list, cost = asyncio.run(plan(
          semaphore=semaphore,
          model_input=plan_input,
          model_name=MODEL_NAME
     ))

     return plan_output_list, cost


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

     """
     # Dataset statistics
     print(f"# of unique tables: {len(table_lake)}")
     print(f"# of dataset: {len(data_dict)}")
     print(f"# of train set: {len(train_data_dict)}")
     print(f"# of validation set: {len(validation_data_dict)}")
     print(f"# of test set: {len(test_data_dict)}")
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
                    table_num = tdx + 1,
                    metadata = table_lake[gold_table_id]['metadata'],
                    header = table_lake[gold_table_id]['header'],
                    cell = table_lake[gold_table_id]['cell']
               ) for tdx, gold_table_id in enumerate(gold_table_id_set)
          ])
          
          display_data = "\n".join([
               (
                    f"Query {str(ddx + 1)} [Entail to table "
                    + ", ".join([
                         str(tdx + 1)
                         for tdx, gold_table_id in enumerate(gold_table_id_set)
                         if gold_table_id in data['entailed_table_id_set']
                    ])
                    + f"] {data['nl_query']}"
               )
               for ddx, data in enumerate(data_list)
          ])

          result_buffer[num].update({
               'display_table': display_table,
               'display_data': display_data
          })

     logger.info("[Done] Display information of gold table set and data set.")

     ###

     # Dataset structure
     # type: SourceDB, SourceWikipedia
     # tables: {table_id, metadata, header, cell}
     # split set: {gold_table_id_set, data_list}
     #                                data_list: {entailed_table_id_set, nl_query, statement}

     for role in ['system', 'user']:
          for task in ['annotate_questions']:
               save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)
     
     # 1. Annotate questions using gold table set metadata and entailed nl queries
     from plans import annotate_questions_task
     from get_shots import get_annotate_questions_task_shots

     plan_output_list, cost = run_plan(
          plan=annotate_questions_task,
          plan_input=[
               {
                    'gold_table_set': [
                         table_lake[gold_table_id]
                         for gold_table_id in gold_table_id_set
                    ],
                    'data_list': data_list,
                    'shots': get_annotate_questions_task_shots()
               }
               for gold_table_id_set, data_list in sampled_data_dict.items()
          ]
     )

     # Parsing
     for num, plan_output in enumerate(plan_output_list):
          try:
               indices = re.findall(r"Annotated question (\d+):", plan_output['response'])
               questions = re.findall(r"Question: (.+?)(?=Difficulty:)", plan_output['response'], re.DOTALL)
               difficulties = re.findall(r"Difficulty: (\w+)", plan_output['response'])
               references = re.findall(r"Reference: \[(.*?)\]", plan_output['response'])

               questions = [question.strip() for question in questions]
               references = [[int(ref) for ref in ref_list.split(", ")] if ref_list != '' else [] for ref_list in references]

               result_buffer[num].update({
                    'llm_output': {
                         int(adx) - 1: {
                              'annotated_question': question,
                              'question_difficulty': diff,
                              'question_reference': ref
                         }
                         for adx, question, diff, ref in zip(indices, questions, difficulties, references)
                    }
               })
          except:
               print(f"[Error] Parsing error at (Annotate questions) - {str(num + 1)}th output.")
     
     ### BUFFER ###
     llm_buffer.append(plan_output_list)
     total_cost['Annotate questions'] = cost
     ### BUFFER ###

     logger.info("[Done] Annotate questions using gold table set metadata and entailed nl queries.")

     # 4. Save to file - temp
     for num, annotation_set in result_buffer.items():
          for mum, annotation in annotation_set['llm_output'].items():
               with open(f'results/storage/{source_type}_{str(num+1)}_{str(mum+1)}_llm.txt', 'w') as file:
                    file.write(result_buffer_format.format(
                         display_table=annotation_set['display_table'],
                         display_data=annotation_set['display_data'],
                         annotated_question=annotation['annotated_question'],
                         question_difficulty=annotation['question_difficulty'],
                         question_reference=str(annotation['question_reference']),
                         annotated_answer=None,
                         verification=None
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
    parser.add_argument('-d', type=str, required=True, choices=["SourceDB", "SourceWikipedia"], help='dataset name')
    parser.add_argument('-n', type=int, required=True, help='number of sampled data')

    args, _ = parser.parse_known_args()
    logger.info(args)

    """
    api_key = '___YOUR_OWN_API_KEY___'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """

    main(
        dataset_name=args.d,
        sample_n=args.n
    )
