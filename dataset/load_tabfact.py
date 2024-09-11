import csv
import json
import os
from dataset_template import Dataset


class TabFactDataset(Dataset):
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
