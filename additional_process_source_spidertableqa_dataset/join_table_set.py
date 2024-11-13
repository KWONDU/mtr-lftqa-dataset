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

    joined_two_three_table_set = [] # total 422 + 138
    joined_four_five_table_set = [] # total 13 + 3

    error_case = []

    for idx, instance in enumerate(primary_key_set):
        if len(instance['gold_table_id_set']) >= 4:
            joined_four_five_table_set.append(instance['gold_table_id_set'])
            continue

        ###

        gold_table_set = [table_lake[table_id] for table_id in instance['gold_table_id_set']] # sorted by table metadata
        join_info_set = instance['join_info_set']

        # Remain and sort unique join information
        unique_join_order_set = set()
        for join_info in join_info_set:
            tb_col1 = (join_info['table_name1'], join_info['column1'])
            tb_col2 = (join_info['table_name2'], join_info['column2'])
            
            if tb_col1[0] < tb_col2[0]: # table name
                unique_join_order_set.add((tb_col1, tb_col2))
            else:
                unique_join_order_set.add((tb_col2, tb_col1))
        unique_join_order_set = list(unique_join_order_set)
        
        if len(instance['gold_table_id_set']) <= len(unique_join_order_set):
            error_case.append(instance['gold_table_id_set']) # manual JOIN
            continue
            
        # case by case
        if len(instance['gold_table_id_set']) == 2: # only 1 JOIN info
            table_name1 = unique_join_order_set[0][0][0]
            column1 = unique_join_order_set[0][0][1]
            table_name2 = unique_join_order_set[0][1][0]
            column2 = unique_join_order_set[0][1][1]
            
            table1 = next(table for table in gold_table_set if table['metadata'].split('|')[1].strip() == table_name1)
            table2 = next(table for table in gold_table_set if table['metadata'].split('|')[1].strip() == table_name2)

            df1 = pd.DataFrame(table1['cell'], columns=[col.lower() for col in table1['header']])
            df2 = pd.DataFrame(table2['cell'], columns=[col.lower() for col in table2['header']])

            ###
            try:
                if column1 == column2:
                    joined_df = pd.merge(df1, df2, on=column1, how='inner')
                else:
                    joined_df = pd.merge(df1, df2, left_on=column1, right_on=column2, how='inner')
            except:
                error_case.append(instance['gold_table_id_set']) # manual JOIN
                continue
            ###
        
        elif len(instance['gold_table_id_set']) == 3:
            if len(unique_join_order_set) == 1:
                error_case.append(instance['gold_table_id_set']) # manual JOIN

            # only 2 JOIN infos
            # JOIN info 1
            table_name1 = unique_join_order_set[0][0][0]
            column1 = unique_join_order_set[0][0][1]
            table_name2 = unique_join_order_set[0][1][0]
            column2 = unique_join_order_set[0][1][1]
            
            table1 = next(table for table in gold_table_set if table['metadata'].split('|')[1].strip() == table_name1)
            table2 = next(table for table in gold_table_set if table['metadata'].split('|')[1].strip() == table_name2)

            df1 = pd.DataFrame(table1['cell'], columns=[col.lower() for col in table1['header']])
            df2 = pd.DataFrame(table2['cell'], columns=[col.lower() for col in table2['header']])

            ###
            try:
                if column1 == column2:
                    joined_df = pd.merge(df1, df2, on=column1, how='inner')
                else:
                    joined_df = pd.merge(df1, df2, left_on=column1, right_on=column2, how='inner')
            except:
                error_case.append(instance['gold_table_id_set']) # manual JOIN
                continue
            ###

            remain_table_name = (set(table['metadata'].split('|')[1].strip() for table in gold_table_set) - {table_name1, table_name2}).pop()

            # JOIN info 2
            table_name3_1 = unique_join_order_set[1][0][0]
            column3_1 = unique_join_order_set[1][0][1]
            table_name3_2 = unique_join_order_set[1][1][0]
            column3_2 = unique_join_order_set[1][1][1]

            if table_name3_1 == remain_table_name:
                table3 = next(table for table in gold_table_set if table['metadata'].split('|')[1].strip() == table_name3_1)
                column3 = column3_1
                joined_column = column3_2
            
            elif table_name3_2 == remain_table_name:
                table3 = next(table for table in gold_table_set if table['metadata'].split('|')[1].strip() == table_name3_2)
                column3 = column3_2
                joined_column = column3_1
            
            else:
                error_case.append(instance['gold_table_id_set']) # manual JOIN

            df3 = pd.DataFrame(table3['cell'], columns=[col.lower() for col in table3['header']])

            ###
            try:
                if joined_column == column3:
                    joined_df = pd.merge(joined_df, df3, on=joined_column, how='inner')
                else:
                    joined_df = pd.merge(joined_df, df3, left_on=joined_column, right_on=column3, how='inner')
            except:
                error_case.append(instance['gold_table_id_set']) # manual JOIN
                continue
            ###

        # case by case
        
        joined_table = {
            'table_id_set': instance['gold_table_id_set'],
            'header': joined_df.columns.tolist(),
            'cell': joined_df.values.tolist()
        }

        joined_two_three_table_set.append(joined_table)
    
    with open('storage/automatic_joined_two_three_table_set.json', 'w') as file:
        json.dump(joined_two_three_table_set, file, indent=4)
    
    error_joined_table_set = error_case + joined_four_five_table_set
    print(f"Error #: {len(error_joined_table_set)}")
    with open('storage/error_joined_table_set.json', 'w') as file:
        json.dump(error_joined_table_set, file, indent=4)
