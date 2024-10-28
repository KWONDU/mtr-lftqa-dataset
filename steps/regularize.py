import copy


def regularize_source_dataset(
        source_dataset: object
    ) -> object:
    """
    Task: regularize source dataset

    [Param]
    source_dataset : object

    [Return]
    regularized_source_dataset : object
    """
    regularized_source_dataset = copy.deepcopy(source_dataset)

    # Regularization 1. transform to string type
    # Regularization 2. replace '–' to '-'
    # Regularization 3. strip spaces

    regularized_source_dataset._tables = [
        {
            'id': table['id'],
            'metadata': table['metadata'].replace('–', '-').strip(),
            'metadata_info': table['metadata_info'].strip(),
            'header': [
                str(col).replace('–', '-').strip() for col in table['header']
            ],
            'cell': [
                [
                    str(cell).replace('–', '-').strip() for cell in row
                ]
                for row in table['cell']
            ]
        }
        for table in source_dataset.tables
    ]

    regularized_source_dataset._instances = [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'data_list': [
                {
                    'entailed_table_id_set': data['entailed_table_id_set'],
                    'nl_query': data['nl_query'].replace('–', '-').strip(),
                    'sql_query': data['sql_query'].replace('–', '-').strip(),
                    'statement': data['statement'].replace('–', '-').strip()
                }
                for data in instance['data_list']
            ]
        }
        for instance in source_dataset.instances
    ]

    return regularized_source_dataset
