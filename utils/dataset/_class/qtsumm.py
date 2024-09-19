import yaml
from datasets import concatenate_datasets, load_dataset
from huggingface_hub import login
from ._dir import current_dir
from ..abstract_dataset import AbstractDataset


class QTSummDataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        with open(f'{current_dir}/source/huggingface_access_token.yaml') as file:
            self.__token = yaml.load(file, Loader=yaml.FullLoader)['token']
            
        self._path = 'yale-nlp/QTSumm'
        self._download_type = 'huggingface'

        login(self.__token)
        self.__dataset = load_dataset(self._path)

        self._tables = self._load_tables()
        self._train = self._load_data('train')
        self._validation = self._load_data('validation')
        self._test = self._load_data('test')
    
    def _load_data(self, split):
        return [
            {
                'gold_tables': [
                    table['id'] for table in self._tables
                    if self._hash_id(data['table']['table_id']) == table['id']
                    ],
                'question': data['query'],
                'answer': data['summary'],
                'answer_type': 'sentence'
            }
            for data in self.__dataset[split]
        ]
    
    def _load_tables(self):
        tables = []

        for data in concatenate_datasets([self.__dataset['train'], self.__dataset['validation'], self.__dataset['test']]):
            table = {
                'id': self._hash_id(data['table']['table_id']),
                'metadata': f"{data['table']['title']}",
                'metadata_info': 'Table title.',
                'header': data['table']['header'],
                'cell': data['table']['rows'],
                'source': None
            }
            tables.append(table)
        # Deduplication tables
        unique_tables = list({table['id']: table for table in tables}.values())
        
        return unique_tables
    
    def __str__(self):
        return '<QTSumm dataset>'
