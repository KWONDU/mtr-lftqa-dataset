import json
from datasets import load_dataset
from .._abstract_dataset import AbstractDataset


class MultiTabQADataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        self._path = [
            ('vaishali/spider-tableQA', 'Spider'),
            ('vaishali/geoQuery-tableQA', 'GeoQuery'),
            ('vaishali/atis-tableQA', 'ATIS')
        ]
        self._download_type = 'huggingface'

        self.__dataset = {
            'train': [],
            'validation': [],
            'test': []
        }

        self._tables = self._load_tables()
        self._train = self._load_data('train')
        self._validation = self._load_data('validation')
        self._test = self._load_data('test')
    
    def _load_data(self, split):
        return [
            {
                'gold_tables': sorted(data['gold_tables']),
                'question': data['question'],
                'answer': (data['query'], data['sub_table']),
                'answer_type': ('SQL', 'table')
            }
            for data in self.__dataset[split]
        ]
    
    def _load_tables(self):
        tables = []

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

                for i, data in enumerate(sub_dataset):
                    ith_gold_tables = [
                        {
                            'id': self._hash_id( # unique table ID
                                f"{source_dataset_name}-"
                                f"{table_name.lower()}-"
                                f"{str(tables_header_cell_list[i][j]['columns'])}"
                                f"{str(tables_header_cell_list[i][j]['data'])}"
                            ),
                            'metadata': table_name.lower(),
                            'metadata_info': 'Table name.',
                            'header': tables_header_cell_list[i][j]['columns'],
                            'cell': tables_header_cell_list[i][j]['data'],
                            'source': source_dataset_name
                        }
                        for j, table_name in enumerate(data['table_names'])
                    ]
                    tables = tables + ith_gold_tables

                    self.__dataset[split].append({
                        'question': data['question'],
                        'query': data['query'],
                        'sub_table': {
                            'header': json.loads(data['answer'])['columns'], # Transform sub-table string to dictionary
                            'cell': json.loads(data['answer'])['data'] # Transform sub-table string to dictionary
                        },
                        'gold_tables': [gold_table['id'] for gold_table in ith_gold_tables]
                    })
            
        # Deduplication tables
        unique_tables = list({table['id']: table for table in tables}.values())

        return unique_tables
    
    def __str__(self):
        return '<MultiTabQA dataset>'
