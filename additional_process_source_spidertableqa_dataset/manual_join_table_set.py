import json
import os
import pandas as pd
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

    with open('storage/primary_key_set.json', 'r') as file:
        primary_key_set = json.load(file)

    with open('storage/automatic_joined_two_three_table_set.json', 'r') as file:
        joined_table_set = json.load(file)

    with open('storage/error_joined_table_set.json', 'r') as file:
        error_join_table_set = json.load(file)
    
    for idx, gold_table_id_set in enumerate(error_join_table_set):
        ###
        local_vars = {
            f'df{tdx + 1}': pd.DataFrame(data=table_lake[table_id]['cell'], columns=[col.lower() for col in table_lake[table_id]['header']]) # lowercase
            for tdx, table_id in enumerate(gold_table_id_set)
        }

        with open(f'storage/join_table/code_{idx}.txt', 'r') as file:
            code = file.read()
        
        exec(
            f"{code}\njoined_df = join_df("
            + ", ".join([f"df{tdx + 1}=df{tdx + 1}" for tdx, _ in enumerate(gold_table_id_set)])
            + ")", 
            globals(),
            local_vars
        )
        ###

        joined_df = local_vars['joined_df']

        # Remove duplicate columns/rows
        columns_to_drop = []
        for col in joined_df.columns:
            if col not in columns_to_drop:
                columns_to_drop.extend([
                    dup_col
                    for dup_col in joined_df.columns
                    if dup_col != col and joined_df[col].equals(joined_df[dup_col])
                ])
        
        joined_df.drop(columns=columns_to_drop)
        joined_df.drop_duplicates()
        # Remove duplicate columns/rows

        joined_table = {
            'table_id_set': gold_table_id_set,
            'header': joined_df.columns.tolist(),
            'cell': joined_df.values.tolist()
        }

        joined_table_set.append(joined_table)

    with open('storage/joined_table_set.json', 'w') as file:
        json.dump(joined_table_set, file, indent=4)
