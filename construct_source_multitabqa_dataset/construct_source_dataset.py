import json
import os
import sys
from collections import defaultdict


class SourceMultiTabQADataset():
    """
    Source dataset - MultiTabQA dataset (Spider subset)

    [Attributes]
    tables    : table lake
    instances : instance set
    """
    def __init__(self):
        self._tables = None
        self._instances = None
    
    @property
    def tables(self):
        return self._tables
    
    @property
    def instances(self):
        return self._instances
    
    def __len__(self):
        return len(self._instances)
    
    def __getitem__(self, key):
        return self._instances[key]
    
    def __str__(self):
        return '<Source dataset - MultiTabQA dataset (Spider subset)>'


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import save_source_dataset
    from utils.display import table_serialization
    return save_source_dataset, table_serialization


if __name__ == '__main__':
    save_source_dataset, table_serialization = import_utils()

    with open('storage/data_set_with_generated_statement.json', 'r') as file:
            data_set_with_generated_statement = json.load(file)

    with open('storage/modified_statement_set_with_human_annotation.json', 'r') as file:
        modified_statement_set = json.load(file)
    
    # Replace modified statements
    for key, statement in modified_statement_set.items():
        idx, jdx = tuple(key.split('-'))
        data_set_with_generated_statement[int(idx)]['data_list'][int(jdx)]['statement'] = statement # modified statement
    
    # Save source dataset
    source_dataset = SourceMultiTabQADataset()

    with open('storage/modified_multitabqa_tables.json', 'r') as file:
        unique_tables = json.load(file)
    
    source_dataset._tables = [
        {
            "id": table['id'],
            "metadata": table['metadata'],
            "metadata_info": table['metadata_info'],
            "header": table['header'],
            "cell": [
                [str(cell) for cell in row]
                for row in table['cell']
            ] # covert all cells to string type
        }
        for table in unique_tables
    ]

    # Single table (entailed table), multi-table (gold table set)
    sorted_data_set_with_generated_statement = sorted(data_set_with_generated_statement, key=lambda x: len(x['gold_table_id_set']), reverse=True)

    instance_set = defaultdict(list)
    for instance in sorted_data_set_with_generated_statement:
        # Multi table, always in front because of sorted list by gold table set length
        if len(instance['gold_table_id_set']) > 1:
            entailed_table_id_set = sorted(instance['gold_table_id_set'])
            for data in instance['data_list']:
                instance_set[tuple(sorted(instance['gold_table_id_set']))].append(
                    {
                        'entailed_table_id_set': entailed_table_id_set,
                        'nl_query': data['nl_query'],
                        'sql_query': data['sql_query'],
                        'statement': data['statement']
                    }
                )
        
        # Single table
        else:
            entailed_table_id = next(table_id for table_id in instance['gold_table_id_set'])
            for data in instance['data_list']:
                for gold_table_id_set, _ in instance_set.items(): # subset
                    if entailed_table_id in gold_table_id_set:
                        instance_set[gold_table_id_set].append(
                            {
                                'entailed_table_id_set': [entailed_table_id],
                                'nl_query': data['nl_query'],
                                'sql_query': data['sql_query'],
                                'statement': data['statement']
                            }
                        )
    
    source_dataset._instances = [
        {
            'gold_table_id_set': list(gold_table_id_set),
            'data_list': data_list
        }
        for gold_table_id_set, data_list in instance_set.items()
    ]

    save_source_dataset(dataset=source_dataset, dataset_name='SourceMultiTabQA')
