import hashlib
import json
import os
import sys
from collections import defaultdict


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    return load_dataset


if __name__ == '__main__':
    load_dataset = import_utils()

    original_dataset_name = 'MultiTabQA'

    # Original dataset: MultiTabQA
    multitabqa = load_dataset(dataset_name=original_dataset_name)
    multitabqa_tables = multitabqa.tables
    multitabqa_train_set = multitabqa.train
    multitabqa_validation_set = multitabqa.validation
    # multitabqa_test_set = multitabqa.test

    ###

    # Only use Spider subset
    # Only use gold multi-table set

    # 1. Modify table_id (table_name > db_id + table_name)
    
    multitabqa_spider_subset_tables = [table for table in multitabqa_tables if table['source'] == 'Spider']

    spider = load_dataset(dataset_name='Spider')
    spider_tables = spider.tables
    spider_train_set = spider.train
    spider_validation_set = spider.validation

    modified_tables = []

    for split, multitabqa_split_set, spider_split_set in [('train', multitabqa_train_set, spider_train_set),
                                                    ('validation', multitabqa_validation_set, spider_validation_set)]:

        # from Spider dataset
        # unique key: (NL query, SQL query, gold table_name set)
        # value: gold db_id set
        spider_unique_key_db_id_set_value_dict = defaultdict(list)
        for spider_data in spider_split_set:
            spider_nl_query = spider_data['question']
            spider_sql_query = spider_data['answer']
            spider_gold_table_name_set = [
                table['metadata'].split('|')[1].lower().strip()
                for table in spider_tables
                if table['id'] in spider_data['gold_tables']
            ]
            spider_unique_key = tuple([spider_nl_query, spider_sql_query, str(sorted(spider_gold_table_name_set))])

            spider_gold_db_id_set = [
                table['metadata'].split('|')[0].lower().strip()
                for table in spider_tables
                if table['id'] in spider_data['gold_tables']
            ]

            spider_unique_key_db_id_set_value_dict[spider_unique_key] += spider_gold_db_id_set
        
        # Initialize split set buffer
        buffer_split_set = []
        
        for multitabqa_data in multitabqa_split_set:
            # unique key: (NL query, SQL query, gold table_name set)
            nl_query = multitabqa_data['question']
            sql_query = next(answer for answer, answer_type in zip(multitabqa_data['answer'], multitabqa_data['answer_type']) if answer_type == 'SQL')
            gold_table_name_set = [
                table['metadata'].lower()
                for table in multitabqa_spider_subset_tables
                if table['id'] in multitabqa_data['gold_tables']
            ]
            unique_key = tuple([nl_query, sql_query, str(sorted(gold_table_name_set))])

            # len(gold_db_id_set) = 0 means the multitabqa_data not in Spider subset (GEOquery, ATIS)
            gold_db_id_set = set(spider_unique_key_db_id_set_value_dict[unique_key])

            for gold_db_id in gold_db_id_set:
                modified_gold_table_set = [
                    {
                        'id': hashlib.sha256(f"{gold_db_id} | {table['metadata']}".encode()).hexdigest(),
                        'metadata': f"{gold_db_id} | {table['metadata']}",
                        'metadata_info': 'Concatenation of database ID and each table name.',
                        'header': table['header'],
                        'cell': table['cell']
                    } for table in multitabqa_spider_subset_tables if table['id'] in multitabqa_data['gold_tables']
                ]
                modified_tables += modified_gold_table_set # Append to buffer

                modified_data = {
                    'gold_table_id_set': sorted([table['id'] for table in modified_gold_table_set]),
                    'nl_query': nl_query,
                    'sql_query': sql_query,
                    'sql_query_result': next(answer for answer, answer_type in zip(multitabqa_data['answer'], multitabqa_data['answer_type']) if answer_type == 'table')
                }
                buffer_split_set.append(modified_data) # Append to buffer
        
        if split == 'train':
            buffer_train_set = buffer_split_set
        elif split == 'validation':
            buffer_validation_set = buffer_split_set

    # Unique tables
    unique_tables = list({table['id']: table for table in modified_tables}.values())

    # 2. Save to storage

    with open('buffer/modified_multitabqa_tables.json', 'w') as file:
        json.dump(unique_tables, file, indent=4)

    for split, buffer_split_set in [('train', buffer_train_set),
                                    ('validation', buffer_validation_set)]:
        
        modified_split_set_dict = defaultdict(list)

        for modified_data in buffer_split_set:
            if len(modified_data['gold_table_id_set']) < 2:
                continue

            modified_split_set_dict[tuple(modified_data['gold_table_id_set'])].append({
                'nl_query': modified_data['nl_query'],
                'sql_query': modified_data['sql_query'],
                'sql_query_result': modified_data['sql_query_result']
            })
        
        modified_split_set = [
            {'gold_table_id_set': list(gold_table_id_set), 'data_list': data_list}
            for gold_table_id_set, data_list in modified_split_set_dict.items()
        ]

        with open(f'buffer/modified_multitabqa_{split}_set.json', 'w') as file:
            json.dump(modified_split_set, file, indent=4)
