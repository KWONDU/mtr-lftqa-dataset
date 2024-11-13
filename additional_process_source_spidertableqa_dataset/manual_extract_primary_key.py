import json
import os
import sys


def load_source_spidertableqa_dataset():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from steps.regularize import regularize_source_dataset
    from utils.dataset import load_source_dataset
    return regularize_source_dataset(source_dataset=load_source_dataset(dataset_name='SourceSpiderTableQA'))


if __name__ == '__main__':
    with open('storage/automatic_primary_key_set.json', 'r') as file:
        primary_key_set = json.load(file)

    with open('storage/error_primary_key_set.json', 'r') as file:
        error_primary_key_set = json.load(file)
    
    source_spidertableqa_dataset = load_source_spidertableqa_dataset()
    table_lake = {table['id']: table for table in source_spidertableqa_dataset.tables}

    error_case = []
    for idx, gold_table_id_set in enumerate(error_primary_key_set):
        instance = next(ins for ins in source_spidertableqa_dataset[:] if ins['gold_table_id_set'] == gold_table_id_set)

        os.system('clear')

        print(f"{idx + 1}/{len(error_primary_key_set)}")

        print()

        print("# Table set")
        for table_id in gold_table_id_set:
            print(f"metadata: {table_lake[table_id]['metadata']}")
            print(f"header: {table_lake[table_id]['header']}")
        
        print()

        print("# SQL queries")
        for data in instance['data_list']:
            print(data['sql_query'])
        
        print()
        
        print("# Annotation")
        join_info_set = []
        joined_table_name_set = set()
        while True:
            table_name1 = input("Table name 1: ")
            column1 = input("Column 1: ")
            table_name2 = input("Table name 2: ")
            column2 = input("Column 2: ")
            join_info_set.append({
                'table_name1': table_name1,
                'column1': column1,
                'table_name2': table_name2,
                'column2': column2
            })
            joined_table_name_set.add(table_name1)
            joined_table_name_set.add(table_name2)

            flag = input("Continue(Y/N)?: ")
            if flag == 'Y':
                continue
            elif flag == 'N':
                break
        
        if len(joined_table_name_set) != len(gold_table_id_set):
            error_case.append(gold_table_id_set)
        else:
            primary_key_set.append({
                'gold_table_id_set': gold_table_id_set,
                'join_info_set': join_info_set
            })
    
    """
    with open('error_case.json', 'w') as file:
        json.dump(error_case, file, indent=4)
    """
    
    with open('storage/primary_key_set.json', 'w') as file:
        json.dump(primary_key_set, file, indent=4)
