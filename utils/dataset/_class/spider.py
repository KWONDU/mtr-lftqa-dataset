import json
from ._dir import current_dir
from .._abstract_dataset import AbstractDataset


class SpiderDataset(AbstractDataset):
    def __init__(self):
        super().__init__()
        self._path = 'source/spider'
        self._download_type = 'local'
        self._tables = self._load_tables()
        self._train = self._load_data('train')
        self._validation = self._load_data('validation')
    
    def _load_data(self, split):
        processed_dataset = []

        with open(f'{current_dir}/{self._path}/{split}.json', 'r') as file:
            data_list = json.load(file)

        for data in data_list:
            gold_db_id = data['db_id'].lower()
            gold_tables_name_original_list = [
                data['query_toks'][pos + 1].lower() for pos, token in enumerate(data['query_toks'])
                if token in ['FROM', 'JOIN'] and data['query_toks'][pos + 1] != '('
                ]
            
            # Error handling with wrong data annotation - cnt: 1
            try:
                processed_data = {
                    'gold_tables': sorted([
                        table['id'] for table in self._tables
                        for gold_table_name_original in gold_tables_name_original_list
                        if self._hash_id(
                            f'{gold_db_id} | {gold_table_name_original}'
                            ) == table['id']
                        ]),
                    'question': data['question'],
                    'answer': data['query'],
                    'answer_type': 'SQL'
                }
                processed_dataset.append(processed_data)
            except Exception as e:
                # print(e)
                continue
        
        return processed_dataset

    def _load_tables(self):
        tables = []

        with open(f'{current_dir}/{self._path}/tables.json', 'r') as file:
            db_list = json.load(file)
        
        for db in db_list:
            column_names_original, db_id, table_names_original = db['column_names_original'], db['db_id'], db['table_names_original']

            tables = tables + [
                {
                    'id': self._hash_id(f'{db_id.lower()} | {table_name.lower()}'),
                    'metadata': f'{db_id.lower()} | {table_name.lower()}',
                    'metadata_info': 'Concatenation of database ID and each table name.',
                    'header': [column_name[1] for column_name in column_names_original if column_name[0] == i],
                    'cell': None,
                    'source': None
                } for i, table_name in enumerate(table_names_original)
            ]
        
        return tables
    
    def __str__(self):
        return '<Spider dataset>'
