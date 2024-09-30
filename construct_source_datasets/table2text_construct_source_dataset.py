import json
import os
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from sentence_transformers import SentenceTransformer
from sklearn.cluster import DBSCAN


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


def cluster_data(id_list, topic_set_list, eps=1e-4, min_samples=1):
    model = SentenceTransformer('paraphrase-MiniLM-L6-v2')

    concatenated_topic_list = [', '.join(topic_set) for topic_set in topic_set_list]

    embeddings = model.encode(concatenated_topic_list, show_progress_bar=True)

    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    clusters = dbscan.fit_predict(embeddings)

    clustered_id_dict = defaultdict(list)
    for idx, each_id in enumerate(id_list):
        each_cluster_idx = clusters[idx]
        clustered_id_dict[str(each_cluster_idx)].append(each_id)
    
    return clustered_id_dict


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset, save_source_dataset
    return load_dataset, save_source_dataset


if __name__ == '__main__':
    load_dataset, save_source_dataset = import_utils()

    original_dataset_name = 'TabFact'
    source_dataset_name = 'SourceTable2Text'

    # Original dataset: TabFact
    tabfact = load_dataset(dataset_name=original_dataset_name)
    tabfact_tables = tabfact.tables
    tabfact_train_set = tabfact.train
    tabfact_validation_set = tabfact.validation
    tabfact_test_set = tabfact.test

    ###

    # 1. Construct page (with corresponding tables) clusters

    match_page_title_to_table_id_set = defaultdict(list)
    for table in tabfact_tables:
        match_page_title_to_table_id_set[table['metadata']].append(table['id'])

    with open('storage/generated_coarse_grained_topic_set_for_each_page.json', 'r') as file:
        topic_set_per_page = json.load(file)

    # Cluster pages with coarse-grained topic set
    page_title_list = [page_title for page_title in topic_set_per_page.keys()]

    page_clusters_with_prev_level_topic_set = {'': page_title_list}

    for cur_level in ['high_level', 'middle_level', 'low_level']:
        try:
            with open(f'storage/page_clusters_with_{cur_level}_topic_set.json', 'r') as file:
                page_clusters_with_prev_level_topic_set = json.load(file)
                continue
        except:
            page_clusters_with_cur_level_topic_set = dict()

            for each_prev_cluster, each_prev_page_title_list in page_clusters_with_prev_level_topic_set.items():
                if each_prev_cluster != '#19':
                    continue
                each_cur_level_topic_set_list = [topic_set_per_page[each_prev_page_title][cur_level] for each_prev_page_title in each_prev_page_title_list]
                print(f"{cur_level}: cluster {each_prev_cluster}")
                each_page_clusters_with_cur_level_topic_set = cluster_data(
                    id_list=each_prev_page_title_list,
                    topic_set_list=each_cur_level_topic_set_list
                    )
                
                for each_cur_cluster, each_cur_page_title_list in each_page_clusters_with_cur_level_topic_set.items():
                    page_clusters_with_cur_level_topic_set[f'{each_prev_cluster}#{each_cur_cluster}'] = each_cur_page_title_list
            
            with open(f'storage/page_clusters_with_{cur_level}_topic_set.json', 'w') as file:
                json.dump(page_clusters_with_cur_level_topic_set, file, indent=4)
            exit()
            page_clusters_with_prev_level_topic_set = page_clusters_with_cur_level_topic_set

    page_clusters_with_coarse_grained_topic_set = page_clusters_with_prev_level_topic_set

    coarse_grained_table_clusters = defaultdict(list)
    for each_cluster, each_page_title_list in page_clusters_with_coarse_grained_topic_set.items():
        for each_page_title in each_page_title_list:
            coarse_grained_table_clusters[each_cluster].append(match_page_title_to_table_id_set[each_page_title])
    
    ###

    train_table_id_set = set(''.join(train_data['gold_tables']) for train_data in tabfact_train_set)
    validation_table_id_set = set(''.join(validation_data['gold_tables'] for validation_data in tabfact_validation_set))
    test_table_id_set = set(''.join(test_data['gold_tables']) for test_data in tabfact_test_set)

    coarse_grained_train_table_clusters = defaultdict(list)
    coarse_grained_validation_table_clusters = defaultdict(list)
    coarse_grained_test_table_clusters = defaultdict(list)
    for each_cluster_id, each_table_id_set in coarse_grained_table_clusters.items():
        for table_id in each_table_id_set:
            if table_id in train_table_id_set:
                coarse_grained_train_table_clusters[each_cluster_id].append(table_id)
            elif table_id in validation_table_id_set:
                coarse_grained_validation_table_clusters[each_cluster_id].append(table_id)
            elif table_id in test_table_id_set:
                coarse_grained_test_table_clusters[each_cluster_id].append(table_id)
    
    # Cluster tables with fine-grained topic set
    with open('generated_fine_grained_topic_set_for_each_table.json', 'r') as file:
        topic_set_per_table = json.load(file)
    
    for split in ['train', 'validation', 'test']:
        coarse_grained_split_table_clusters = coarse_grained_train_table_clusters if split == 'train' \
            else coarse_grained_validation_table_clusters if split == 'validation' \
                else coarse_grained_test_table_clusters if split == 'test' \
                    else None
        fine_grained_split_table_clusters = dict()

        for each_cluster, each_table_id_list in coarse_grained_split_table_clusters:
            each_fine_grained_topic_set_list = [topic_set_per_table[each_table_id] for each_table_id in each_table_id_list]
            each_table_clusters_with_fine_grained_topic_set = cluster_data(
                id_list=each_table_id_list,
                topic_set_list=each_fine_grained_topic_set_list
            )

            for each_fine_grained_cluster, each_fine_grained_table_id_list in each_table_clusters_with_fine_grained_topic_set.items():
                fine_grained_split_table_clusters[f'{each_cluster}#{each_fine_grained_cluster}'] = each_fine_grained_table_id_list
        
        if split == 'train':
            fine_grained_train_table_clusters = fine_grained_split_table_clusters
        elif split == 'validation':
            fine_grained_validation_table_clusters = fine_grained_split_table_clusters
        elif split == 'test':
            fine_grained_test_table_clusters = fine_grained_split_table_clusters
    exit()

    # 2. Construct source dataset
    source_dataset = SourceTable2TextDataset()

    match_table_id_to_annotated_data = defaultdict(list)
    for data in tabfact[:]:
        match_table_id_to_annotated_data[''.join(data['gold_tables'])].append(data)

    source_dataset._tables = tabfact_tables
    
    source_train_set = list()
    for _, gold_table_id_set in fine_grained_train_table_clusters.items():
        tabfact_data_list = [match_table_id_to_annotated_data[gold_table_id] for gold_table_id in gold_table_id_set]
        topic_set = [
            topic
            for gold_table_id in gold_table_id_set
            for topic in topic_set_per_table[gold_table_id]
        ]
        source_train_set.append(
            {
                'gold_table_id_set': gold_table_id_set,
                'data_list': [
                    next(answer for answer, answer_type in zip(tabfact_data['answer'], tabfact_data['answer_type']) if answer_type)
                    for tabfact_data in tabfact_data_list
                ],
                'topic_set': topic_set
            }
        )
    source_dataset._train = source_train_set

    source_validation_set = list()
    for _, gold_table_id_set in fine_grained_validation_table_clusters.items():
        tabfact_data_list = [match_table_id_to_annotated_data[gold_table_id] for gold_table_id in gold_table_id_set]
        topic_set = [
            topic
            for gold_table_id in gold_table_id_set
            for topic in topic_set_per_table[gold_table_id]
        ]
        source_validation_set.append(
            {
                'gold_table_id_set': gold_table_id_set,
                'data_list': [
                    next(answer for answer, answer_type in zip(tabfact_data['answer'], tabfact_data['answer_type']) if answer_type)
                    for tabfact_data in tabfact_data_list
                ],
                'topic_set': topic_set
            }
        )
    source_dataset._validation = source_validation_set

    source_test_set = list()
    for _, gold_table_id_set in fine_grained_test_table_clusters.items():
        tabfact_data_list = [match_table_id_to_annotated_data[gold_table_id] for gold_table_id in gold_table_id_set]
        topic_set = [
            topic
            for gold_table_id in gold_table_id_set
            for topic in topic_set_per_table[gold_table_id]
        ]
        source_test_set.append(
            {
                'gold_table_id_set': gold_table_id_set,
                'data_list': [
                    next(answer for answer, answer_type in zip(tabfact_data['answer'], tabfact_data['answer_type']) if answer_type)
                    for tabfact_data in tabfact_data_list
                ],
                'topic_set': topic_set
            }
        )
    source_dataset._test = source_test_set

    # 3. Save dataset to file
    save_source_dataset(dataset=source_dataset, dataset_name=source_dataset_name)
