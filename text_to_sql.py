from util import load_dataset


def extract_multi_gold_tables_per_data(data_list):
    return [
        data for data in data_list
        if len(data.gold_tables) > 1
    ]


if __name__ == '__main__':
    origin_dataset = load_dataset('MultiTabQA')
    print(f'source dataset: {origin_dataset}')

    table_lake = origin_dataset.tables
    dataset = extract_multi_gold_tables_per_data(origin_dataset[:])

    print(len(dataset))
