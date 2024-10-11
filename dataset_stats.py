import argparse
import csv
import importlib
from utils.dataset import load_dataset, load_source_dataset, save_dataset


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', type=str, help='dataset type (source or original)', required=True)
    args, _ = parser.parse_known_args()

    if args.t == 'original':
        for source_type in ["SourceDB", "SourceWikipedia"]:
            dataset = load_source_dataset(dataset_name=source_type)
            tables = dataset._tables
            train_set = dataset._train or []
            validation_set = dataset._validation or []
            test_set = dataset._test or []

            print(f"About {source_type} dataset . . .")

            print(f"Instance # of train set: {len(train_set)}")
            print(f"Instance # of validation set: {len(validation_set)}")
            print(f"Instance # of test set: {len(test_set)}")
            print(f"Total instance #: {len(dataset[:])}")

            gold_table_id_set_len = sorted([len(data['gold_table_id_set']) for data in dataset[:]])
            data_list_len = sorted([len(data['data_list']) for data in dataset[:]])

            print(f"Avg. # of gold tables per instance: {sum(_ for _ in gold_table_id_set_len) / len(gold_table_id_set_len):.2f}")
            print(f"Med. # of gold tables per instance: {gold_table_id_set_len[len(gold_table_id_set_len) // 2]}")

            print(f"Avg. # of statements per instance: {sum(_ for _ in data_list_len) / len(data_list_len):.2f}")
            print(f"Med. # of statements per instance: {data_list_len[len(data_list_len) // 2]}")
            print(f"Total # of statements: {sum(_ for _ in data_list_len)}")
    
    elif args.t == 'source':
        dataset_list_header = ['dataset name', 'dataset type', 'source data']
        dataset_list = [
            ('Spider', 'text-to-SQL', 'Online, DB, WikiSQL'),
            ('WikiSQL', 'text-to-SQL', 'Wikipedia'),
            ('ToTTo', 'table-to-text', 'Wikipedia'),
            ('QTSumm', 'table-to-text', 'LogicNLG, ToTTo'),
            ('MultiTabQA', 'table QA', 'Spider, GEOquery, ATIS'),
            ('WikiTableQuestions', 'table QA', 'Wikipedia'),
            ('FeTaQA', 'table QA', 'ToTTo'),
            ('NQ-tables', 'open-domain table QA', 'Wikipedia'),
            ('Open-WikiTable', 'open-domain table QA', 'WikiSQL, WikiTableQuestions'),
            ('TabFact', 'table-based fact verification', 'Wikipedia')
            ]

        dataset_stats_list_header = [
            'dataset name',
            'dataset type',
            'dataset size',
            '# of unique tables',
            '# of table per data',
            'answer type',
            'source data'
            ]
        dataset_stats_list = []

        for dataset_name, dataset_type, source_data in dataset_list:
            try:
                dataset = load_dataset(dataset_name=dataset_name)
                if not dataset:
                    # Forced access to private data
                    module = importlib.import_module('utils.dataset._class')
                    dataset_class = getattr(module, f"{dataset_name.replace('-', '')}Dataset") # remove '-'
                    dataset = dataset_class()
                    save_dataset(dataset=dataset, dataset_name=dataset_name)
            except Exception as e:
                print(e)
                continue
            
            dataset_size = len(dataset)
            num_of_unique_tables = len(dataset.tables)
            num_of_tables_per_data_list = [len(dataset[idx]['gold_tables']) for idx in range(dataset_size)]
            num_of_tables_per_data = f'{sum(num_of_tables_per_data_list) / len(num_of_tables_per_data_list):.2f}'
            answer_type = dataset[0]['answer_type']

            dataset_stats_list.append(
                [
                    dataset_name,
                    dataset_type,
                    dataset_size,
                    num_of_unique_tables,
                    num_of_tables_per_data,
                    answer_type,
                    source_data
                ]
            )
            print(f"Complete {dataset_name} dataset statistics calculation!")
        
        dataset_stats_list.append(
            [
                'Our dataset',
                'open-domain table QA',
                '...',
                '...',
                '> 2',
                'sentence',
                'MultiTabQA, ...'
            ]
        )
        
        # save to result file
        with open('results/dataset_statistics.csv', mode='w', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            writer.writerow(dataset_stats_list_header)
            writer.writerows(dataset_stats_list)

    else:
        exit()
