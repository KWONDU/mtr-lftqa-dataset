import json
import os
import sys
from collections import defaultdict


class SourceDBDataset():
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
        return '<Source DB domain Dataset>'


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
    source_db_domain_dataset = SourceDBDataset()

    with open('storage/modified_multitabqa_tables.json', 'r') as file:
        unique_tables = json.load(file)
    
    source_db_domain_dataset._tables = unique_tables

    data_set = defaultdict(list)

    # Single table (entailed table), multi-table (gold table set)
    sorted_data_set_with_generated_statement = sorted(data_set_with_generated_statement, key=lambda x: len(x['gold_table_id_set']), reverse=True)

    for instance in sorted_data_set_with_generated_statement:
        for data in instance['data_list']:
            
            if len(instance['gold_table_id_set']) > 1: # multi-table
                data_set[(data['split'], tuple(instance['gold_table_id_set']))].append(
                    {
                        'entailed_table_id_set': instance['gold_table_id_set'],
                        'statement': data['statement']
                    }
                )
            
            else: # single table
                entailed_table_id = next(table_id for table_id in instance['gold_table_id_set'])
                for (split, gold_table_id_tuple), _ in data_set.items():
                    if entailed_table_id in gold_table_id_tuple:
                        data_set[(split, gold_table_id_tuple)].append(
                            {
                                'entailed_table_id_set': [entailed_table_id],
                                'statement': data['statement']
                            }
                        )

    source_db_domain_dataset._train = [
        {'gold_table_id_set': list(gold_table_id_set), 'data_list': data_list}
        for (split, gold_table_id_set), data_list in data_set.items()
        if split == 'train'
    ]

    source_db_domain_dataset._validation = [
        {'gold_table_id_set': list(gold_table_id_set), 'data_list': data_list}
        for (split, gold_table_id_set), data_list in data_set.items()
        if split == 'validation'
    ]

    save_source_dataset(dataset=source_db_domain_dataset, dataset_name='SourceDB')
