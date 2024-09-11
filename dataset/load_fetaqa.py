from dataset_template import Dataset
from datasets import concatenate_datasets, load_dataset


class FeTaQADataset(Dataset):
    def __init__(self):
        super().__init__()
        self._path = 'DongfuJiang/FeTaQA'
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
                    if data['table_source_json'] == table['temp_key']
                    ],
                'question': data['question'],
                'answer': data['answer'],
                'answer_type': 'sentence'
            }
            for data in dataset
        ]
    
    def __load_tables(self):
        tables = []

        dataset = load_dataset(self._path)

        for data in concatenate_datasets([dataset['train'], dataset['validation'], dataset['test']]):
            table = {
                'metadata': f"{data['table_page_title']} | {data['table_section_title']}",
                'metadata_info': 'Concatenation of page title and section title.',
                'header': data['table_array'][0],
                'cell': data['table_array'][1:],
                'temp_key': data['table_source_json']
            }
            tables.append(table)
        # Deduplication tables
        unique_tables = list({table['temp_key']: table for table in tables}.values())
        
        return unique_tables, dataset
