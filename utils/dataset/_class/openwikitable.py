import json
from ._dir import current_dir
from .._abstract_dataset import AbstractDataset


class OpenWikiTableDataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        self._path = 'source/openwikitable'
        self._download_type = 'local'
        self._tables = self._load_tables()
        self._train = self._load_data('train')
        self._validation = self._load_data('validation')
        self._test = self._load_data('test')
    
    def _load_data(self, split):
        with open(f'{current_dir}/{self._path}/{split}.json', 'r') as file:
            data_dict = json.load(file)
        
        data_info_keys = list(data_dict.keys())
        data_info_before_transposed_list = [
            [
                data_info for data_info in data_info_values.values()
            ]
            for data_info_values in data_dict.values()
        ]
        data_info_list = [list(data_info) for data_info in zip(*data_info_before_transposed_list)]
        dataset = [dict(zip(data_info_keys, data_info)) for data_info in data_info_list]

        return [
            {
                'gold_tables': [
                    table['id'] for table in self._tables
                    if self._hash_id(data['original_table_id']) == table['id']
                    ],
                'question': data['question'],
                'answer': (data['answer'], data['sql']),
                'answer_type': ('word', 'SQL')
            }
            for data in dataset
        ]
    
    def _load_tables(self):
        with open(f'{current_dir}/{self._path}/tables.json', 'r') as file:
            tables_dict = json.load(file)
        
        tables_info_keys = list(tables_dict.keys())
        tables_info_before_transposed_list = [
            [
                table_info for table_info in tables_info_values.values()
            ]
            for tables_info_values in tables_dict.values()
        ]
        tables_info_list = [list(tables_info) for tables_info in zip(*tables_info_before_transposed_list)]
        tables_list = [dict(zip(tables_info_keys, tables_info)) for tables_info in tables_info_list]
        
        return [
            {
                'id': self._hash_id(table['original_table_id']),
                'metadata': f"{table['page_title']} | {table['section_title']} | {table['caption']}",
                'metadata_info': 'Concatenation of page title, section title and table caption.',
                'header': table['header'],
                'cell': table['rows'],
                'source': table['dataset']
            }
            for table in tables_list
        ]
    
    def __str__(self):
        return '<Open-WikiTable dataset>'
