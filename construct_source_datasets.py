import hashlib
from collections import defaultdict
from utils.dataset import load_dataset, save_source_dataset


class SourceText2SQLDataset():
    def __init__(self):
        self._tables = None
        self._train = None
        self._validation = None
        self._test = None
    
    def _get_single_item(self, idx):
        if idx < self._train_len:
            return self._train[idx]
        elif idx - self._train_len < self._validation_len:
            return self._validation[idx - self._train_len]
        elif idx - self._train_len - self._validation_len < self._test_len:
            return self._test[idx - self._train_len - self._validation_len]
        else:
            return None
    
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
    
    def __str__(self):
        return '<SourceText2SQL Dataset>'


class SourceTable2TextDataset():
    def __init__(self):
        self._tables = None
        self._train = None
        self._validation = None
        self._test = None

    def _get_single_item(self, idx):
        if idx < self._train_len:
            return self._train[idx]
        elif idx - self._train_len < self._validation_len:
            return self._validation[idx - self._train_len]
        elif idx - self._train_len - self._validation_len < self._test_len:
            return self._test[idx - self._train_len - self._validation_len]
        else:
            return None
    
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
    
    def __str__(self):
        return '<SourceTable2Text Dataset>'


def hash_id(id):
    return hashlib.sha256(id.encode()).hexdigest()


if __name__ == '__main__':
    dataset_name_info_list = [
        ('MultiTabQA', 'SourceText2SQL'),
        ('TabFact', 'SourceTable2Text')
    ]

    for (original_dataset_name, source_data_list_name) in dataset_name_info_list:
        if load_dataset(dataset_name=source_data_list_name):
            continue

        original_dataset = load_dataset(dataset_name=original_dataset_name)

        original_tables = original_dataset.tables
        original_train_dataset = original_dataset.train
        original_validation_dataset = original_dataset.validation
        original_test_dataset = original_dataset.test

        # Construct SourceText2SQL dataset
        if original_dataset_name == 'MultiTabQA':
            # 1. Modify table id (table_name -> db_id + table_name)
            
            # Extract Spider subset of MultiTabQA dataset
            original_tables = [table for table in original_tables if table['source'] == 'Spider']

            # Load Spider dataset
            spider = load_dataset(dataset_name='Spider')
            spider_tables = spider.tables
            spider_train_dataset = spider.train
            spider_validation_dataset = spider.validation

            # Initialization source_tables
            source_tables = []

            # Initialization source_data_list
            idx_split_match = {0: 'train', 1: 'validation'}
            source_data_list = [[], []]

            # For each split (train, validation) ...
            for idx, spider_dataset in enumerate([spider_train_dataset, spider_validation_dataset]):
                original_dataset = original_train_dataset if idx_split_match[idx] == 'train' \
                    else original_validation_dataset if idx_split_match[idx] == 'validation' \
                        else None

                spider_unique_key_dict = defaultdict(list)
                for spider_data in spider_dataset:
                    spider_question = spider_data['question']
                    spider_sql_query = spider_data['answer']
                    spider_gold_table_metadata_set = [table['metadata'] for table in spider_tables if table['id'] in spider_data['gold_tables']]
                    spider_unique_key_dict[tuple([
                        spider_question,
                        spider_sql_query,
                        str(sorted([spider_gold_table_metadata.split('|')[1].lower().strip() for spider_gold_table_metadata in spider_gold_table_metadata_set])) # table name
                    ])] += [spider_gold_table_metadata.split('|')[0].lower().strip() for spider_gold_table_metadata in spider_gold_table_metadata_set] # db id

                for original_data in original_dataset:
                    # Construct unique key to classify corresponding tables and data
                    original_question = original_data['question']
                    original_sql_query = next(answer for answer, answer_type in zip(original_data['answer'], original_data['answer_type']) if answer_type=='SQL')
                    original_gold_table_name_set = [table['metadata'].lower() for table in original_tables if table['id'] in original_data['gold_tables']]
                    unique_key = tuple([original_question, original_sql_query, str(sorted(original_gold_table_name_set))])

                    # len(gold_db_id_set) == 0 means corresponding data is not in Spider dataset (GEOquery, ATIS)
                    gold_db_id_set = set(spider_unique_key_dict[unique_key])
                    
                    for gold_db_id in gold_db_id_set:
                        # Modified gold table set
                        gold_table_set = [
                            {
                                'id': hash_id(f"{gold_db_id} | {table['metadata']}"),
                                'metadata': f"{gold_db_id} | {table['metadata']}",
                                'metadata_info': 'Concatenation of database ID and each table name.',
                                'header': table['header'],
                                'cell': table['cell']
                            } for table in original_tables if table['id'] in original_data['gold_tables']
                        ]

                        # Append to buffer
                        source_tables += gold_table_set # gold table set
                        source_data_list[idx].append({
                            'gold_tables': sorted([gold_table['id'] for gold_table in gold_table_set]),
                            'question': original_data['question'],
                            'answer': original_data['answer'],
                            'answer_type': original_data['answer_type']
                        }) # modified data
                
            source_tables = list({table['id']: table for table in source_tables}.values())

            # 2. Construct source dataset
            source_dataset = SourceText2SQLDataset()

            final_source_train_dataset = defaultdict(list)
            final_source_validation_dataset = defaultdict(list)

            # Only have gold multi-table set
            for source_data in source_data_list[0]:
                if len(source_data['gold_tables']) < 2:
                    continue

                final_source_train_dataset[tuple(source_data['gold_tables'])].append({
                    'question': source_data['question'],
                    'sql_query': next(answer for answer, answer_type in zip(source_data['answer'], source_data['answer_type']) if answer_type=='SQL'),
                    'sql_extraction': next(answer for answer, answer_type in zip(source_data['answer'], source_data['answer_type']) if answer_type=='table')
                })
            final_source_train_dataset = [
                {'gold_table_id_set': sorted(list(gold_table_ids)), 'data_list': data_list}
                for gold_table_ids, data_list in final_source_train_dataset.items()
            ]
            
            # Only have gold multi-table set
            for source_data in source_data_list[1]:
                if len(source_data['gold_tables']) < 2:
                    continue

                final_source_validation_dataset[tuple(source_data['gold_tables'])].append({
                    'question': source_data['question'],
                    'sql_query': next(answer for answer, answer_type in zip(source_data['answer'], source_data['answer_type']) if answer_type=='SQL'),
                    'sql_extraction': next(answer for answer, answer_type in zip(source_data['answer'], source_data['answer_type']) if answer_type=='table')
                })
            final_source_validation_dataset = [
                {'gold_table_id_set': sorted(list(gold_table_ids)), 'data_list': data_list}
                for gold_table_ids, data_list in final_source_validation_dataset.items()
            ]

            source_dataset._tables = source_tables
            source_dataset._train = final_source_train_dataset
            source_dataset._validation = final_source_validation_dataset

            # 3. Save dataset to file
            save_source_dataset(dataset=source_dataset, dataset_name='SourceText2SQL')

        # originalTable2Text
        elif original_dataset_name == 'TabFact':
            None
        
        else:
            break
