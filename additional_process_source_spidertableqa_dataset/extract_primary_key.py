import json
import os
import re
import sys


def load_source_spidertableqa_dataset():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from steps.regularize import regularize_source_dataset
    from utils.dataset import load_source_dataset
    return regularize_source_dataset(source_dataset=load_source_dataset(dataset_name='SourceSpiderTableQA'))


if __name__ == '__main__':
    source_spidertableqa_dataset = load_source_spidertableqa_dataset()
    table_lake = {table['id']: table for table in source_spidertableqa_dataset.tables}

    primary_key_set = []
    not_join_gold_table_id_set = []

    for instance in source_spidertableqa_dataset[:]:
        gold_table_id_set = instance['gold_table_id_set']
        table_name_set = [
            table_lake[table_id]['metadata'].split('|')[1].strip()
            for table_id
            in gold_table_id_set
        ]

        join_info_set = set()
        joined_table_name_set = set()

        try:
            for data in instance['data_list']:
                sql_query = data['sql_query']

                # 1. Find table name alising
                table_name_match = dict() # same gold table set but different aliasing can exist
                matches = re.findall(r'(\w+)\sAS\s(\w+)', sql_query)
                for match in matches:
                    table_name, alias = match
                    if table_name.lower() in table_name_set:
                        table_name_match[alias.lower()] = table_name.lower()
                
                if len(table_name_match) != len(table_name_set):
                    continue

                # 2. Find primary key between tables
                matches = re.findall(r'(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)', sql_query)
                for match in matches:
                    alias1, column1, alias2, column2 = match
                    table_name1 = table_name_match[alias1.lower()]
                    table_name2 = table_name_match[alias2.lower()]
                    join_info_set.add((table_name1, column1.lower(), table_name2, column2.lower()))
                    joined_table_name_set.add(table_name1)
                    joined_table_name_set.add(table_name2)
        except:
            not_join_gold_table_id_set.append(gold_table_id_set)
            continue

        if len(join_info_set) == 0 or len(joined_table_name_set) != len(table_name_set):
            not_join_gold_table_id_set.append(gold_table_id_set)
        else:
            primary_key_set.append(
                {
                    'gold_table_id_set': gold_table_id_set,
                    'join_info_set': [
                        {
                            'table_name1': join_info[0],
                            'column1': join_info[1],
                            'table_name2': join_info[2],
                            'column2': join_info[3]
                        }
                        for join_info in join_info_set
                    ]
                }
            )

    with open('storage/automatic_primary_key_set.json', 'w') as file:
        json.dump(primary_key_set, file, indent=4)
    
    with open('storage/not_join_gold_table_id_set.json', 'w') as file:
        json.dump(not_join_gold_table_id_set, file, indent=4)
