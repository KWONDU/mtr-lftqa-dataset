from util import load_dataset


def extract_multi_gold_tables_per_data(data_list):
    return [
        data for data in data_list
        if len(data['gold_tables']) > 1
    ]


if __name__ == '__main__':
    origin_dataset = load_dataset('MultiTabQA')

    table_lake = [
        table for table in origin_dataset.tables
        if table['source'] == 'Spider'
        ]
    dataset = [
        all([
            True if gold_table_id in [_['id'] for _ in table_lake] else False
            for gold_table_id in data['gold_tables']
        ])
        for data in origin_dataset[:]
    ]

    print(len(table_lake))
    print(len(dataset))
