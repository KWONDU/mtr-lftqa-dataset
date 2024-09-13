import jsonlines


class WikiSQLDataset():
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
        return '<WikiSQL dataset>'
    
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
