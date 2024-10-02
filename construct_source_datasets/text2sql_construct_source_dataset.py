import asyncio
import json
import sys
import os
from tqdm.asyncio import tqdm_asyncio


class SourceText2SQLDataset():
    def __init__(self):
        self._tables = None
        self._train = None
        self._validation = None
        self._test = None
    
    def _get_single_item(self, idx):
        if idx < self._train_len:
            return self._train[idx]
        elif idx - self._train_len < self._validation_len:
            return self._validation[idx - self._train_len]
        elif idx - self._train_len - self._validation_len < self._test_len:
            return self._test[idx - self._train_len - self._validation_len]
        else:
            return None
    
    @property
    def tables(self):
        return self._tables
    
    @property
    def train(self):
        return self._train

    @property
    def validation(self):
        return self._validation

    @property
    def test(self):
        return self._test

    @property
    def _train_len(self):
        return len(self._train) if self._train else 0
    
    @property
    def _validation_len(self):
        return len(self._validation) if self._validation else 0
    
    @property
    def _test_len(self):
        return len(self._test) if self._test else 0

    def __len__(self):
        return self._train_len + self._validation_len + self._test_len
    
    def __getitem__(self, key):
        if isinstance(key, slice):
            items = []
            for idx in range(key.start or 0, key.stop or len(self), key.step or 1):
                item = self._get_single_item(idx)
                if item:
                    items.append(item)
                else:
                    return items
            return items
        else:
            return self._get_single_item(key)
    
    def __str__(self):
        return '<SourceText2SQL Dataset>'


async def generate_short_form_answer(model_input, model_name, semaphore, table_serialization, get_async_openai_response):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='generate_short_form_answer'),
            user_prompt=load_prompt(role='user', task='generate_short_form_answer').format(
                nl_query=input_data['nl_query'],
                sql_query=input_data['sql_query'],
                sql_query_result=table_serialization(
                    metadata=None,
                    header=input_data['sql_query_result']['header'],
                    cell=input_data['sql_query_result']['cell']
                )
            ),
            model_name=model_name,
            key=input_data['key']
        ) for input_data in model_input
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks):
        try:
            model_output = await task
            model_output_list.append(model_output)
        except Exception as e:
            print(e)

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import save_source_dataset
    from utils.display import table_serialization
    from utils.openai import load_prompt, save_prompt, get_async_openai_response
    return save_source_dataset, table_serialization, load_prompt, save_prompt, get_async_openai_response


if __name__ == '__main__':
    save_source_dataset, table_serialization, load_prompt, save_prompt, get_async_openai_response = import_utils()

    source_dataset = SourceText2SQLDataset()

    with open('storage/modified_multitabqa_tables.json', 'r') as file:
        unique_tables = json.load(file)
    source_dataset._tables = unique_tables

    match_query_short_form_answer_dict = dict()

    for split in ['train', 'validation']:
        with open(f'storage/modified_multitabqa_{split}_set.json', 'r') as file:
            split_set = json.load(file)
        
        for role in ['system', 'user']:
            save_prompt(file_path=f'prompts/{role}/generate_short_form_answers.txt', role=role, task='generate_short_form_answers')
        
        # Generate short-form answer from each NL-SQL query pair and SQL query result

        model_name = 'gpt-3.5-turbo'

        task_input = [
            {
                'nl_query': data['nl_query'],
                'sql_query': data['sql_query'],
                'sql_query_result': data['sql_query_result'],
                'key': (idx, jdx)
            }
            for idx, instance in enumerate(split_set)
            for jdx, data in enumerate(instance['data_list'])
        ]

        try:
            with open(f'buffer/generate_short_form_answer_{split}_set_llm.json', 'r') as file:
                task_output_list = json.load(file)
        except:
            semaphore = asyncio.Semaphore(100)
            task_output_list, cost = asyncio.run(generate_short_form_answer(
                model_input=task_input,
                model_name=model_name,
                semaphore=semaphore,
                table_serialization=table_serialization,
                get_async_openai_response=get_async_openai_response
            ))

            print(f'Total {split} set cost: ${cost:.2f}')

            with open(f'buffer/generate_short_form_answer_{split}_set_llm.json', 'w') as file:
                json.dump(task_output_list, file, indent=4)

        task_response_list = [
            sorted_task_output['response']
            for sorted_task_output in sorted(task_output_list, key=lambda x: x['key'])
        ]

        source_split_set = []
        pos = 0

        for idx, instance in enumerate(split_set):
            data_list = []
            for jdx, data in enumerate(instance['data_list']):
                data_list.append(task_response_list[pos])
                match_query_short_form_answer_dict[(split, idx, jdx)] = {
                    'nl_query': data['nl_query'],
                    'sql_query': data['sql_query'],
                    'sql_query_result': data['sql_query_result'],
                    'short_form_answer': task_response_list[pos]
                }
                pos += 1
            source_split_set.append({
                'gold_table_id_set': instance['gold_table_id_set'],
                'data_list': data_list
            })

        if split == 'train':
            source_dataset._train = source_split_set
        elif split == 'validation':
            source_dataset._validation = source_split_set
        else:
            exit()
    
    save_source_dataset(dataset=source_dataset, dataset_name='sourcetext2sql')

    match_query_short_form_answer_dict = {str(key): value for key, value in match_query_short_form_answer_dict.items()}
    with open('storage/matched_query_short_form_answer.json', 'w') as file:
        json.dump(match_query_short_form_answer_dict, file, indent=4)
