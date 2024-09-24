import hashlib
import json
import numpy as np
import os
import sys
from collections import defaultdict
from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
from sklearn.neighbors import NearestNeighbors
from tqdm import tqdm

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from utils.dataset import load_dataset, load_source_dataset, save_source_dataset


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
        if load_source_dataset(dataset_name=source_data_list_name):
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

            # Initialization
            source_tables = []
            source_train_dataset = []
            source_validation_dataset = []

            # For each split (train, validation) ...
            for split in ['train', 'validation']:
                original_dataset, spider_dataset = (original_train_dataset, spider_train_dataset) if split == 'train' \
                    else (original_validation_dataset, spider_validation_dataset) if split == 'validation' \
                        else (None, None)
                source_split_dataset = []

                # {unique_key(question, SQL query, gold table name set): gold db ID set}
                spider_unique_key_dict = defaultdict(list)
                for spider_data in spider_dataset:
                    spider_question = spider_data['question']
                    spider_sql_query = spider_data['answer']
                    spider_gold_table_name_set = [
                        table['metadata'].split('|')[1].lower().strip()
                        for table in spider_tables
                        if table['id'] in spider_data['gold_tables']
                    ]
                    spider_gold_db_id_set = [
                        table['metadata'].split('|')[0].lower().strip()
                        for table in spider_tables
                        if table['id'] in spider_data['gold_tables']
                    ]

                    spider_unique_key_dict[
                        tuple([spider_question, spider_sql_query, str(sorted(spider_gold_table_name_set))])
                        ] += spider_gold_db_id_set

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
                        source_split_dataset.append({
                            'gold_tables': sorted([gold_table['id'] for gold_table in gold_table_set]),
                            'question': original_data['question'],
                            'answer': original_data['answer'],
                            'answer_type': original_data['answer_type']
                        }) # modified data
                
                if split == 'train':
                    source_train_dataset += source_split_dataset
                elif split == 'validation':
                    source_validation_dataset += source_split_dataset
                
            # Unique tables
            source_tables = list({table['id']: table for table in source_tables}.values())

            # 2. Construct source dataset
            source_dataset = SourceText2SQLDataset()
            source_dataset._tables = source_tables # tables

            for split in ['train', 'validation']:
                source_split_dataset = source_train_dataset if split == 'train' \
                    else source_validation_dataset if split == 'validation' \
                        else None
                
                final_source_split_dataset = defaultdict(list)
            
                for source_data in source_split_dataset:
                    if len(source_data['gold_tables']) < 2: # Only have gold multi-table set
                        continue

                    final_source_split_dataset[tuple(source_data['gold_tables'])].append({
                        'question': source_data['question'],
                        'sql_query': next(answer for answer, answer_type in zip(source_data['answer'], source_data['answer_type']) if answer_type == 'SQL'),
                        'sql_extraction': next(answer for answer, answer_type in zip(source_data['answer'], source_data['answer_type']) if answer_type == 'table')
                    })
                
                if split == 'train':
                    source_dataset._train = [
                        {'gold_table_id_set': sorted(list(gold_table_ids)), 'data_list': data_list}
                        for gold_table_ids, data_list in final_source_split_dataset.items()
                    ] # train set
                elif split == 'validation':
                    source_dataset._validation = [
                        {'gold_table_id_set': sorted(list(gold_table_ids)), 'data_list': data_list}
                        for gold_table_ids, data_list in final_source_split_dataset.items()
                    ] # validation set

        # Construct SourceTable2Text dataset
        elif original_dataset_name == 'TabFact':
            # 0. Extract categories and save to file
            with open('wikipedia_categories_for_each_table.json', 'r') as file:
                table_id_key_categories_value_dict = json.load(file)

            # Initialization
            source_tables = []
            source_train_dataset = []
            source_validation_dataset = []
            source_test_dataset = []

            # Pre-trained Sentence BERT model
            model = SentenceTransformer('paraphrase-MiniLM-L12-v2')

            original_table_dict = {table['id']: table for table in original_tables}

            # For each set (train, validation, test)
            for split in ['train', 'validation', 'test']:
                original_split_dataset = original_train_dataset if split == 'train' \
                    else original_validation_dataset if split == 'validation' \
                        else original_test_dataset if split == 'test' \
                            else None

                original_split_tables = [
                    original_table_dict[gold_table_id]
                    for data in original_split_dataset
                    for gold_table_id in data['gold_tables']
                ]
                original_split_tables = list({table['id']: table for table in original_split_tables}.values())

                # 1. Cluster pages to construct gold multi-table set
                page_title_categories_dict = {}
                for table in original_split_tables:
                    try: # tables with same metadata (page title) have same categories
                        page_title_categories_dict[table['metadata']] = table_id_key_categories_value_dict[table['id']]
                    except: # error handling (only single page, 'elissa sursara')
                        continue

                ### Clustering start ###
                page_title_list = list(page_title_categories_dict.keys())
                category_set_list = list(page_title_categories_dict.values())

                page_title_categories_reverse_dict = {tuple(v): k for k, v in page_title_categories_dict.items()}

                try: # embedding json file
                    with open('category_set_embedding_per_each_page.json', 'rb') as file:
                        category_set_embedding_list = [np.array(_) for _ in json.loads(file.read().decode('utf-8'))]
                except:
                    category_set_embedding_list = [
                        np.mean(model.encode(category_set), axis=0)
                        for category_set in tqdm(category_set_list)
                    ]
                    with open('category_set_embedding_per_each_page.json', 'wb') as file:
                        file.write(json.dumps([_.tolist() for _ in category_set_embedding_list]).encode('utf-8'))
                
                wikipedia_category_topics = [['Research', 'Library science'], ['Culture', 'The arts'], ['Geography', 'Places'], ['Health', 'Self-care', 'Health care occupations'], ['History', 'Events'], ['Human activities'], ['Mathematics and logic'], ['Science', 'Natural sciences', 'Nature'], ['People', 'Personal life', 'Self', 'Surnames'], ['Philosophy', 'Thought'], ['Religion', 'Belief'], ['Society', 'Social Sciences'], ['Technology', 'Applied sciences']]

                nearsest_neighbors = NearestNeighbors(n_neighbors=1)
                nearsest_neighbors.fit([np.mean(model.encode(topic_set), axis=0) for topic_set in wikipedia_category_topics])

                distances, indices = nearsest_neighbors.kneighbors(category_set_embedding_list)
                assigned_clusters = indices.flatten()

                knn_json = {tuple(topic): [] for topic in wikipedia_category_topics}
                for page_idx, topic_idx in enumerate(assigned_clusters):
                    topic = wikipedia_category_topics[topic_idx]
                    knn_json[tuple(topic)].append(category_set_list[page_idx])
                with open('knn_each_page_with_wikipedia_topic.txt', 'w') as file:
                    for k, v_list in knn_json.items():
                        file.write(f'###\t{str(k)}\t### ({len(v_list)})\n')
                        for v in v_list:
                            file.write(f'>\t{page_title_categories_reverse_dict[tuple(v)]}\n')
                        file.write('\n')
                exit()
                
                agg_clustering = AgglomerativeClustering(n_clusters=1000, linkage='average').fit(category_set_embedding_list)
                labels = agg_clustering.labels_

                split_clusters = [[] for _ in range(len(set(labels)))]
                for idx, label in enumerate(labels):
                    split_clusters[label].append({tuple(sorted(category_set_list[idx])): page_title_list[idx]})
                ### Clustering end ###

                print(split_clusters[0])
                print(len(split_clusters))
                temp_dict = defaultdict(int)
                for label, cluster in enumerate(split_clusters):
                    temp_dict[label] = len(cluster)
                for k, v in temp_dict.items():
                    print(f'{k}: {v}')
                exit()

                original_split_dataset_dict = defaultdict(list)
                for data in original_split_dataset:
                    gold_table_id = data['gold_tables'][0] # TabFact dataset have single gold table
                    statement = next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type == 'sentence')
                    entail = next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type == 'T/F')

                    if entail:
                        original_split_dataset_dict[gold_table_id].append(statement)
                
                source_split_dataset = []

                # For each cluster
                for cluster in split_clusters:
                    for categories, page_title in cluster.items(): # Add table information about categories
                        # Extract gold table id set from page title
                        gold_table_id_set = [table['id'] for table in original_split_tables if table['metadata'] == page_title]

                        for gold_table_id in gold_table_id_set:
                            source_tables.append(original_table_dict[gold_table_id].update({'categories': list(categories)}))
                            
                        # Consider table set of each cluster as gold multi-table set
                        source_split_dataset.append({
                            'gold_table_id_set': sorted(gold_table_id_set), # Gold multi-tabe set
                            'data_list': [
                                data
                                for gold_table_id in gold_table_id_set
                                for data in original_split_dataset_dict[gold_table_id]
                                ] # Corresponding entailed statements
                        })
                
                # each spilt set is empty so + operator is better (shallow copy problem)
                if split == 'train':
                    source_train_dataset += source_split_dataset
                elif split == 'validation':
                    source_validation_dataset += source_split_dataset
                elif split == 'test':
                    source_test_dataset += source_split_dataset
            
            # Unique tables (just in case)
            source_tables = list({table['id']: table for table in source_tables}.values())
            
            # 2. Construct source dataset
            source_dataset = SourceTable2TextDataset()

            source_dataset._tables = source_tables
            source_dataset._train = source_train_dataset
            source_dataset._validation = source_validation_dataset
            source_dataset._test = source_test_dataset

        else:
            break

        # 3. Save dataset to file
        save_source_dataset(dataset=source_dataset, dataset_name=source_data_list_name)
