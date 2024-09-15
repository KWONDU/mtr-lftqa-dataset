import hashlib
import json
from datasets import load_dataset


class MultiTabQADataset():
    def __init__(self):
        super().__init__()
        self._path = [
            ('vaishali/spider-tableQA', 'Spider'),
            ('vaishali/geoQuery-tableQA', 'GeoQuery'),
            ('vaishali/atis-tableQA', 'ATIS')
        ]
        self._download_type = 'huggingface'
        self._tables, dataset = self.__load_tables()
        self._train = self.__load_data(dataset['train'])
        self._validation = self.__load_data(dataset['validation'])
        self._test = self.__load_data(dataset['test'])
    
    def __load_data(self, dataset):
        return [
            {
                'gold_tables': sorted(data['gold_tables']),
                'question': data['question'],
                'answer': (data['query'], data['sub_table']),
                'answer_type': ('SQL', 'table')
            }
            for data in dataset
        ]
    
    def __load_tables(self):
        tables = []
        processed_dataset = {
            'train': [],
            'validation': [],
            'test': []
        }

        for path, source_dataset_name in self._path:
            dataset = load_dataset(path)

            for split in ['train', 'validation', 'test']:
                # Spider-tableQA dataset doesn't have test set
                try:
                    sub_dataset = dataset[split]
                except:
                    continue
            
                tables_header_cell_list = [
                    [json.loads(json_table) for json_table in data['tables']]
                    for data in sub_dataset
                ]
            
                tables = tables + [
                    {
                        'id': hashlib.sha256(f'{source_dataset_name}-{table_name}'.encode()).hexdigest(),
                        'metadata': table_name,
                        'metadata_info': 'Table name.',
                        'header': tables_header_cell_list[i][j]['columns'],
                        'cell': tables_header_cell_list[i][j]['data'],
                        'source': source_dataset_name
                    }
                    for i, data in enumerate(sub_dataset)
                    for j, table_name in enumerate(data['table_names'])
                ]
            
                processed_dataset[split] = processed_dataset[split] + [
                    {
                        'question': data['question'],
                        'query': data['query'],
                        'sub_table': {
                            'header': json.loads(data['answer'])['columns'], # Transform sub-table string to dictionary
                            'cell': json.loads(data['answer'])['data'] # Transform sub-table string to dictionary
                        },
                        'gold_tables': [
                            hashlib.sha256(f'{source_dataset_name}-{table_name}'.encode()).hexdigest()
                            for table_name in data['table_names']
                            ]
                    }
                    for data in sub_dataset
                ]
            
        # Deduplication tables
        unique_tables = list({table['id']: table for table in tables}.values())

        return unique_tables, processed_dataset
    
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
        return '<MultiTabQA dataset>'
    
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
