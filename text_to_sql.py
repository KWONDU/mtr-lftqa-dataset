from util import load_dataset


def extract_multi_gold_tables_per_data(data_list):
    return [
        data for data in data_list
        if len(data['gold_tables']) > 1
    ]


def extract_spider_subset(table_lake, dataset):
    non_spider_tables_idx_list = [
        idx for idx, table in enumerate(table_lake)
        if '(w/ Spider dataset)' not in table['metadata_info']
    ]

    non_spider_data_idx_list = []
    for table_idx in non_spider_tables_idx_list:
        for data_idx, data in enumerate(dataset):
            if table_idx in data['gold_tables']:
                non_spider_data_idx_list.append(data_idx)
            else:
                dataset[data_idx]['gold_tables'] = [
                    gold_table_idx - 1 if gold_table_idx > table_idx else gold_table_idx
                    for gold_table_idx in data['gold_tables']
                ]

    source_table_lake = [
        table for idx, table in enumerate(table_lake)
        if idx not in non_spider_tables_idx_list
        ]

    source_dataset = [
        data for idx, data in enumerate(dataset)
        if idx not in non_spider_data_idx_list
    ]

    return source_table_lake, source_dataset


if __name__ == '__main__':
    origin_dataset = load_dataset('MultiTabQA')
    print(f'source dataset: {origin_dataset}')

    table_lake = origin_dataset.tables
    dataset = extract_multi_gold_tables_per_data(origin_dataset[:])
    
    print(f'# of gold-tables per data > 1\n- table lake size: {len(table_lake)}\n- dataset size: {len(dataset)}')

    source_table_lake, source_dataset = extract_spider_subset(table_lake, dataset)

    print(f'# of gold-tables per data > 1 w/ Spider\n- table lake size: {len(source_table_lake)}\n- dataset size: {len(source_dataset)}')
