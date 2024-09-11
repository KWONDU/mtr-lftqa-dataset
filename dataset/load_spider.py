import json
from dataset_template import Dataset


class SpiderDataset(Dataset):
    def __init__(self):
        super().__init__()
        self._path = 'source/spider'
        self._download_type = 'local'
        self._tables, table_name_original_match_dict = self.__load_tables()
        self._train = self.__load_data('train', table_name_original_match_dict)
        self._validation = self.__load_data('validation', table_name_original_match_dict)
    
    def __load_data(self, split, table_name_original_match_dict):
        processed_dataset = []

        with open(f'{self._path}/{split}.json', 'r') as file:
            data_list = json.load(file)

        for data in data_list:
            gold_db_id = data['db_id']
            gold_tables_name_original_list = [
                data['query_toks'][pos + 1] for pos, token in enumerate(data['query_toks'])
                if token in ['FROM', 'JOIN'] and data['query_toks'][pos + 1] != '('
                ]
            gold_tables_name_original_list = list(set(gold_tables_name_original_list))
            
            # Error handling with wrong data annotation - cnt: 1
            try:
                processed_data = {
                    'gold_tables': sorted([
                        idx for idx, table in enumerate(self._tables)
                        for gold_table_name_original in gold_tables_name_original_list
                        if table['metadata'] == f'{gold_db_id} | {table_name_original_match_dict[gold_table_name_original.lower()]}'
                        ]),
                    'question': data['question'],
                    'answer': data['query'],
                    'answer_type': 'SQL'
                }
            except Exception as e:
                # print(e)
                continue
            processed_dataset.append(processed_data)
        
        return processed_dataset

    def __load_tables(self):
        tables = []
        table_name_original_match_dict = {}

        with open(f'{self._path}/tables.json', 'r') as file:
            db_list = json.load(file)
        
        for db in db_list:
            column_names, db_id, table_names = db['column_names'], db['db_id'], db['table_names']
            table_names_original = db['table_names_original']

            for i, table_name in enumerate(table_names):
                table = {
                    'metadata': f'{db_id} | {table_name}',
                    'metadata_info': "Concatenation of database ID and each table name.",
                    'header': [column_name[1] for column_name in column_names if column_name[0] == i],
                    'cell': None
                }
                table_name_original = table_names_original[i]
                table_name_original_match_dict[table_name_original.lower()] = table_name
                tables.append(table)
        
        return tables, table_name_original_match_dict
