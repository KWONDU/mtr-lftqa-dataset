import csv
import json
import os
from ._dir import current_dir
from ..abstract_dataset import AbstractDataset


class TabFactDataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        self._path = 'source/tabfact'
        self._download_type = 'local'
        self._tables = self._load_tables()
        self._train = self._load_data('train')
        self._validation = self._load_data('validation')
        self._test = self._load_data('test')
    
    def _load_data(self, split):
        processed_dataset = []
        
        with open(f'{current_dir}/{self._path}/{split}.json', 'r') as file:
            data_dict = json.load(file)

            for gold_table_id, data in data_dict.items():
                statements_list, labels_list, page_title = data[0], data[1], data[2]
                hash_table_id = self._hash_id(gold_table_id)
                for idx, table in enumerate(self._tables):
                    if hash_table_id == table['id']:
                        self._tables[idx]['metadata'] = page_title

                for statement, label in zip(statements_list, labels_list):
                    processed_data = {
                        'gold_tables': [hash_table_id],
                        'question': None,
                        'answer': (statement, bool(label)),
                        'answer_type': ('sentence', 'T/F')
                    }
                    processed_dataset.append(processed_data)
        
        return processed_dataset
    
    def _load_tables(self):
        tables = []

        for table_id in os.listdir(f'{current_dir}/{self._path}/tables'):
            with open(f'{current_dir}/{self._path}/tables/{table_id}', 'r', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter='#')

                table_header = []
                table_cell = []

                for i, row in enumerate(reader):
                    if i == 0:
                        table_header = row
                    else:
                        table_cell.append(row)

            table = {
                'id': self._hash_id(table_id),
                'metadata': None, # Will be filled at _load_data method
                'metadata_info': "Page title.",
                'header': table_header,
                'cell': table_cell,
                'source': None
            }
            tables.append(table)
        
        return tables
    
    def __str__(self):
        return '<TabFact dataset>'
