import yaml
from dataset_template import Dataset
from datasets import concatenate_datasets, load_dataset
from huggingface_hub import login


class QTSummDataset(Dataset):
    def __init__(self):
        super().__init__()
        with open('huggingface_access_token.yaml') as file:
            self.__token = yaml.load(file, Loader=yaml.FullLoader)['token']
        self._path = 'yale-nlp/QTSumm'
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
                    idx for idx, table in enumerate(self._tables)
                    if data['table']['table_id'] == table['temp_key']
                    ],
                'question': data['query'],
                'answer': data['summary'],
                'answer_type': 'sentence'
            }
            for data in dataset
        ]
    
    def __load_tables(self):
        tables = []

        login(self.__token)
        dataset = load_dataset(self._path)

        for data in concatenate_datasets([dataset['train'], dataset['validation'], dataset['test']]):
            table = {
                'metadata': f"{data['table']['title']}",
                'metadata_info': 'Table title.',
                'header': data['table']['header'],
                'cell': data['table']['rows'],
                'temp_key': data['table']['table_id']
            }
            tables.append(table)
        # Deduplication tables
        unique_tables = list({table['temp_key']: table for table in tables}.values())
        
        return unique_tables, dataset
