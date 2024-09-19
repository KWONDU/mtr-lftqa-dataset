from datasets import concatenate_datasets, load_dataset
from ..abstract_dataset import AbstractDataset


class FeTaQADataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        self._path = 'DongfuJiang/FeTaQA'
        self._download_type = 'huggingface'

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
                    if self._hash_id(data['table_source_json']) == table['id']
                    ],
                'question': data['question'],
                'answer': data['answer'],
                'answer_type': 'sentence'
            }
            for data in self.__dataset[split]
        ]
    
    def _load_tables(self):
        tables = []

        for data in concatenate_datasets([self.__dataset['train'], self.__dataset['validation'], self.__dataset['test']]):
            table = {
                'id': self._hash_id(data['table_source_json']),
                'metadata': f"{data['table_page_title']} | {data['table_section_title']}",
                'metadata_info': 'Concatenation of page title and section title.',
                'header': data['table_array'][0],
                'cell': data['table_array'][1:],
                'source': 'ToTTo'
            }
            tables.append(table)
        # Deduplication tables
        unique_tables = list({table['id']: table for table in tables}.values())
        
        return unique_tables
    
    def __str__(self):
        return '<FeTaQA dataset>'
