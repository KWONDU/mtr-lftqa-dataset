import csv
import glob
import importlib
import os
import sys


if __name__ == '__main__':
    dataset_list_header = ['dataset name', 'dataset type', 'source data']
    dataset_list = [
        ('Spider', 'text-to-SQL', 'Online, DB, WikiSQL'),
        ('WikiSQL', 'text-to-SQL', 'Wikipedia'),
        ('MultiTabQA', 'table QA', 'Spider, GEOquery, ATIS'),
        ('WikiTableQuestions', 'table QA', 'Wikipedia'),
        ('FeTaQA', 'table QA', 'ToTTo'),
        ('QTSumm', 'table QA', 'LogicNLG, ToTTo'),
        ('TableBench', 'table QA', 'WikiTableQuestions, TabFact, FeTaQA, ...'),
        ('Open-WikiTable', 'open-domain table QA', 'WikiSQL, WikiTableQuestions'),
        ('NQ-Tables', 'open-domain table QA', 'Natural Questions'),
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
        processed_dataset_name = dataset_name.replace('-', '') # remove '-'

        try:
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            import util_dill
            dill_files = glob.glob(f'dump/{processed_dataset_name.lower()}*.pkl')
            if dill_files:
                dataset = util_dill.load_large_object(dill_files, '', f'load_{processed_dataset_name.lower()}')
            else:
                module = importlib.import_module(f'load_{processed_dataset_name.lower()}')
                dataset = getattr(module, f'{processed_dataset_name}Dataset')()
                util_dill.save_large_object(dataset, f'dump/{processed_dataset_name.lower()}')
        except Exception as e:
            print(f"[{dataset_name} dataset] {e}")
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
    
    # save to csv file
    with open('../results/dataset_statistics.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(dataset_stats_list_header)
        writer.writerows(dataset_stats_list)
