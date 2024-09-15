import logging
import random
from tqdm import tqdm
from util import load_dataset


def dataset_preprocessing(dataset_name):
    origin_dataset = load_dataset(dataset_name)

    # Extract table and data which source dataset is Spider dataset
    table_lake = [
        table for table in origin_dataset.tables
        if table['source'] == 'Spider'
        ]
    dataset = [
        data for data in origin_dataset[:]
        if all([
            True if gold_table_id in [_['id'] for _ in table_lake] else False
            for gold_table_id in data['gold_tables']
        ])
    ]

    # table_dict: {table_id: table}
    # data_dict: {gold_table_ids: data (# > 1)}
    table_dict = {
        table['id']: table
        for table in table_lake
    }
    data_dict = {}
    for data in dataset:
        if len(data['gold_tables']) == 1:
            continue
        if tuple(data['gold_tables']) not in data_dict:
            data_dict[tuple(data['gold_tables'])] = [data]
        else:
            data_dict[tuple(data['gold_tables'])].append(data)
    data_dict = {
        gold_table_ids: data_list for gold_table_ids, data_list in data_dict.items()
        if len(data_list) > 1
    }

    cnt_tables_per_set = [len(_) for _ in data_dict.keys()]
    cnt_data_per_set = [len(_) for _ in data_dict.values()]
    logger.info(
        "MultiTabQA dataset (only Spider dataset)" + "\n" +
        f"> Table lake size: {len(table_dict)}" + "\n" +
        f"> Dataset size (w/ Gold multi-tables set - data list (# > 1)): {len(data_dict)}" + "\n" +
        f"> Avg. # of gold multi-tables per each set: {sum(cnt_tables_per_set) / len(cnt_tables_per_set):.2f}" + "\n" +
        f"> Avg. # of data per each set: {sum(cnt_data_per_set) / len(cnt_data_per_set):.2f}"
    )

    return table_dict, data_dict


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    table_dict, data_dict = dataset_preprocessing('MultiTabQA')

    random.seed(42)
    sample_ten_data_dict = dict(random.sample(list(data_dict.items()), 10))

    for gold_table_ids, data_list in tqdm(sample_ten_data_dict.items()):
        # 1. Generate high-level question templates from MultiTabQA question - SQL extraction pairs
        None
        # 2. Give feedback & verify about generated high-level question templates
        None
        # 3. Generate answer for each high-level question
        None
