class Dataset():
    def __init__(self):
        self._path = None # dataset download path
        self._download_type = None # huggingface or local
        """
            [
                {
                    'metadata': {text},
                    'metadata_info': {how construct metadata, text},
                    'header': {list},
                    'cell': {2d list}
                },
                . . .
            ]
        """
        self._tables = None # table lake
        """
            [
                {
                    'gold_tables': {list of gold table indices},
                    'question': {question, str},
                    'answer': {answer, str or tuple},
                    'answer_type':{type of answer, str or tuple}
                },
                . . .
            ]
        """
        self._train = None # train set
        self._validation = None # validation set
        self._test = None # test set
    
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

    def __getitem__(self, idx):
        if idx < self._train_len:
            return self._train[idx]
        elif idx - self._train_len < self._validation_len:
            return self._validation[idx - self._train_len]
        elif idx - self._train_len - self._validation_len < self._test_len:
            return self._test[idx - self._train_len - self._validation_len]
        else:
            return None
