import jsonlines
from ._dir import current_dir
from ..abstract_dataset import AbstractDataset


class WikiSQLDataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        self._path = 'source/wikisql'
        self._download_type = 'local'
        self._tables = self._load_tables()
        self._train = self._load_data('train')
        self._validation = self._load_data('validation')
        self._test = self._load_data('test')
    
    def _load_data(self, split):
        with jsonlines.open(f'{current_dir}/{self._path}/{split}.jsonl') as file:
            dataset = [data for data in file.iter()]
        
        return [
            {
                'gold_tables': [
                    table['id'] for table in self._tables
                    if self._hash_id(data['table_id']) == table['id']
                ],
                'question': data['question'],
                'answer': None,
                'answer_type': 'SQL'
            }
            for data in dataset
        ]
    
    def _load_tables(self):
        tables_list = []

        with jsonlines.open(f'{current_dir}/{self._path}/tables.jsonl') as file:
            tables_list = [table for table in file.iter()]

        return [
            {
                'id': self._hash_id(table['id']),
                'metadata': None,
                'metadata_info': None,
                'header': table['header'],
                'cell': table['rows'],
                'source': None
            }
            for table in tables_list
        ]
    
    def __str__(self):
        return '<WikiSQL dataset>'
