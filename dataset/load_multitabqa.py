import json
from dataset_template import Dataset
from datasets import load_dataset


class MultiTabQADataset(Dataset):
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
        self.__delete_temp_key_from_tables()
        
    def __delete_temp_key_from_tables(self):
        for table in self._tables:
            if 'temp_key' in table:
                del table['temp_key']
    
    def __load_data(self, dataset):
        return [
            {
                'gold_tables': [
                    idx for table_name in data['table_names_list']
                    for idx, table in enumerate(self._tables)
                    if table_name == table['temp_key']
                ],
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
                        'metadata': data['table_names'],
                        'metadata_info': 'Table name.',
                        'header': tables_header_cell_list[i][j]['columns'],
                        'cell': tables_header_cell_list[i][j]['data'],
                        'temp_key': f'{source_dataset_name}-{table_name}'
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
                        'table_names_list': [f'{source_dataset_name}-{table_name}' for table_name in data['table_names']],
                    }
                    for data in sub_dataset
                ]
            
        # Deduplication tables
        unique_tables = list({table['temp_key']: table for table in tables}.values())

        return unique_tables, processed_dataset
