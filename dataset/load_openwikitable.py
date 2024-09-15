import hashlib
import json


class OpenWikiTableDataset():
    def __init__(self):
        super().__init__()
        self._path = 'source/openwikitable'
        self._download_type = 'local'
        self._tables = self.__load_tables()
        self._train = self.__load_data('train')
        self._validation = self.__load_data('validation')
        self._test = self.__load_data('test')
    
    def __load_data(self, split):
        with open(f'{self._path}/{split}.json', 'r') as file:
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
                    if hashlib.sha256(data['original_table_id'].encode()).hexdigest() == table['id']
                    ],
                'question': data['question'],
                'answer': (data['answer'], data['sql']),
                'answer_type': ('word', 'SQL')
            }
            for data in dataset
        ]
    
    def __load_tables(self):
        with open(f'{self._path}/tables.json', 'r') as file:
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
                'id': hashlib.sha256(table['original_table_id'].encode()).hexdigest(),
                'metadata': f"{table['page_title']} | {table['section_title']} | {table['caption']}",
                'metadata_info': 'Concatenation of page title, section title and table caption.',
                'header': table['header'],
                'cell': table['rows'],
                'source': table['dataset']
            }
            for table in tables_list
        ]
    
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
        return '<Open-WikiTable dataset>'
    
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
