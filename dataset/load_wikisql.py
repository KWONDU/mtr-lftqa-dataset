import jsonlines
from dataset_template import Dataset


class WikiSQLDataset(Dataset):
    def __init__(self):
        super().__init__()
        self._path = 'source/wikisql'
        self._download_type = 'local'
        self._tables = self.__load_tables()
        self._train = self.__load_data('train')
        self._validation = self.__load_data('validation')
        self._test = self.__load_data('test')
        self.__delete_temp_key_from_tables()
        
    def __delete_temp_key_from_tables(self):
        for table in self._tables:
            if 'temp_key' in table:
                del table['temp_key']
    
    def __load_data(self, split):
        with jsonlines.open(f'{self._path}/{split}.jsonl') as file:
            dataset = [data for data in file.iter()]
        
        return [
            {
                'gold_tables': [
                    idx for idx, table in enumerate(self._tables)
                    if data['table_id'] == table['temp_key']
                ],
                'question': data['question'],
                'answer': None,
                'answer_type': 'SQL'
            }
            for data in dataset
        ]
    
    def __load_tables(self):
        tables_list = []

        with jsonlines.open(f'{self._path}/tables.jsonl') as file:
            tables_list = [table for table in file.iter()]

        return [
            {
                'metadata': None,
                'metadata_info': None,
                'header': table['header'],
                'cell': table['rows'],
                'temp_key': table['id']
            }
            for table in tables_list
        ]
