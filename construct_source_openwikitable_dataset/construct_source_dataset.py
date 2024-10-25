import json
import os
import sys
from tqdm import tqdm


class SourceOpenWikiTableDataset():
    """
    Source dataset - Open-WikiTable dataset

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
        return '<Source dataset - Open-WikiTable dataset>'


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset, save_source_dataset
    return load_dataset, save_source_dataset


if __name__ == '__main__':
    load_dataset, save_source_dataset = import_utils()

    # Statement construction -> question (without ?, ., !): answer list
    # All Open-WikiTable tables exist at single split set

    openwikitable = load_dataset(dataset_name='Open-WikiTable')
    
    with open('storage/gold_table_set.json', 'r') as file:
        gold_table_set = json.load(file)
    
    with open('storage/generated_statements.json', 'r') as file:
        generated_statements = json.load(file)

    source_dataset = SourceOpenWikiTableDataset()

    source_dataset._tables = [
        {
            'id': table['id'],
            'metadata': table['metadata'],
            'metadata_info': table['metadata_info'],
            'header': table['header'],
            'cell': table['cell']
        }
        for table in openwikitable.tables
    ]

    instance_set = {
        tuple(gt_set['gold_table_id_set']): []
        for gt_set in gold_table_set
    }

    for statement, data in tqdm(zip(generated_statements, openwikitable[:]), total=len(openwikitable)):
        entailed_table_id = next(t_id for t_id in data['gold_tables'])
        for gold_table_id_set, _ in instance_set.items():
            if entailed_table_id in gold_table_id_set:
                instance_set[gold_table_id_set].append(
                    {
                        'entailed_table_id_set': [entailed_table_id],
                        'nl_query': data['question'],
                        'sql_query': next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type == 'SQL'),
                        'statement': statement
                    }
                )
    
    source_dataset._instances = [
        {
            'gold_table_id_set': list(gold_table_id_set),
            'data_list': data_list
        }
        for gold_table_id_set, data_list in instance_set.items()
    ]

    save_source_dataset(dataset=source_dataset, dataset_name='SourceOpenWikiTable')
