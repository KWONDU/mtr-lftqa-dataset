import csv
import dill
import importlib
import os


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

    Flag, target_dataset_name = False, 'MultiTabQA'
    if Flag:
        dataset_list = [dataset for dataset in dataset_list if dataset[0] == target_dataset_name]

    for dataset_name, dataset_type, source_data in dataset_list:
        processed_dataset_name = dataset_name.replace('-', '')
        if Flag:
            module = importlib.import_module(f'dataset.load_{processed_dataset_name.lower()}')
            dataset = getattr(module, f'{processed_dataset_name}Dataset')()
            print(dataset_name)
            print(dataset.tables[0])
            print(dataset[0])
            exit()

        try:
            if os.path.exists(f'{os.getcwd()}/dump/{processed_dataset_name.lower()}.dill'):
                with open(f'dump/{processed_dataset_name.lower()}.dill', 'rb') as file:
                    dataset = dill.load(file)
            else:
                module = importlib.import_module(f'load_{processed_dataset_name.lower()}')
                dataset = getattr(module, f'{processed_dataset_name}Dataset')()
                with open(f'dump/{processed_dataset_name.lower()}.dill', 'wb') as file:
                    dill.dump(dataset, file)
        except:
            print(f"{dataset_name} dataset class doesn't exist!")
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
    
    # save to csv file
    with open('dataset_stats.csv', mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(dataset_stats_list_header)
        writer.writerows(dataset_stats_list)
