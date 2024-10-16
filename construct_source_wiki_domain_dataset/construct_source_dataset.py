import json
import os
import sys
from collections import defaultdict


class SourceWikipediaDataset():
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
        return '<Source Wikipedia domain Dataset>'


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
    
    # gold_table_set = sorted([_ for _ in gold_table_set if _[0]['type'] == 1], key=lambda x: len(x))

    ### TEMP: len(gold table set) < 5 + 1 ###

    gold_table_set = [_ for _ in gold_table_set if len(_) < 5 + 1]
    
    sourcewiki = SourceWikipediaDataset()

    sourcewiki._tables = [
        {
            'id': table['id'],
            'metadata': table['metadata'],
            'metadata_info': table['metadata_info'],
            'header': table['header'],
            'cell': table['cell']
        }
        for table in openwikitable.tables
    ]

    full_gold_table_id_set = [[table['id'] for table in _] for _ in gold_table_set]
    full_dataset = defaultdict(list)
    for statement, data in zip(generated_statements, openwikitable[:]):
        gold_table_id = next(table_id for table_id in data['gold_tables'])
        full_dataset[gold_table_id].append({
            'nl_query': data['question'],
            'statement': statement
        })

    for split, split_set in [
        ('train', openwikitable.train),
        ('validation', openwikitable.validation),
        ('test', openwikitable.test)
        ]:
        source_split_set = []

        split_table_id_lake = [
            next(table_id for table_id in data['gold_tables'])
            for data in split_set
        ]

        for gold_table_id_set in full_gold_table_id_set:
            if not all(gold_table_id in split_table_id_lake for gold_table_id in gold_table_id_set):
                continue

            data_list = [
                {
                    'entailed_table_id_set': [gold_table_id],
                    'nl_query': data['nl_query'],
                    'statement': data['statement']
                }
                for gold_table_id in gold_table_id_set
                for data in full_dataset[gold_table_id]
            ]

            source_split_set.append(
                {'gold_table_id_set': gold_table_id_set, 'data_list': data_list}
            )
        
        if split == 'train':
            sourcewiki._train = source_split_set
        elif split == 'validation':
            sourcewiki._validation = source_split_set
        elif split == 'test':
            sourcewiki._test = source_split_set
        else:
            continue
    ###

    save_source_dataset(dataset=sourcewiki, dataset_name='SourceWikipedia')
