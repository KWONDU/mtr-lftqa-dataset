import csv
import json
import os


class TabFactDataset():
    def __init__(self):
        super().__init__()
        self._path = 'source/tabfact'
        self._download_type = 'local'
        self._tables, table_id_idx_match_dict = self.__load_tables()
        self._train = self.__load_data('train', table_id_idx_match_dict)
        self._validation = self.__load_data('validation', table_id_idx_match_dict)
        self._test = self.__load_data('test', table_id_idx_match_dict)
    
    def __load_data(self, split, table_id_idx_match_dict):
        processed_dataset = []
        
        with open(f'{self._path}/{split}.json', 'r') as file:
            data_dict = json.load(file)

            for gold_table_id, data in data_dict.items():
                statements_list, labels_list, page_title = data[0], data[1], data[2]
                gold_table_idx = table_id_idx_match_dict[gold_table_id]
                self._tables[gold_table_idx]['metadata'] = page_title

                for statement, label in zip(statements_list, labels_list):
                    processed_data = {
                        'gold_tables': [gold_table_idx],
                        'question': None,
                        'answer': (statement, bool(label)),
                        'answer_type': ('sentence', 'T/F')
                    }
                    processed_dataset.append(processed_data)
        
        return processed_dataset
    
    def __load_tables(self):
        tables = []
        table_id_idx_match_dict = {}

        for idx, table_id in enumerate(os.listdir(f'{self._path}/tables')):
            table_id_idx_match_dict[table_id] = idx
            with open(f'{self._path}/tables/{table_id}', 'r', encoding='utf-8') as file:
                reader = csv.reader(file, delimiter='#')

                table_header = []
                table_cell = []

                for i, row in enumerate(reader):
                    if i == 0:
                        table_header = row
                    else:
                        table_cell.append(row)

            table = {
                'metadata': None, # Will be filled at __load_data method
                'metadata_info': "Page title.",
                'header': table_header,
                'cell': table_cell
            }
            tables.append(table)
        
        return tables, table_id_idx_match_dict

    @property
    def download_type(self):
        return self._download_type

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
    
    def __str__(self):
        return '<TabFact dataset>'
    
    def _get_single_item(self, idx):
        if idx < self._train_len:
            return self._train[idx]
        elif idx - self._train_len < self._validation_len:
            return self._validation[idx - self._train_len]
        elif idx - self._train_len - self._validation_len < self._test_len:
            return self._test[idx - self._train_len - self._validation_len]
        else:
            return None
    
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
