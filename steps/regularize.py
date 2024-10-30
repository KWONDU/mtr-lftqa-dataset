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

    # Regularization 1. sort gold table set by table metadata
    # Regularization 2. type conversion to string
    # Regularization 3. replace '–' (en dash) to '-' (hyphen)
    # Regularization 4. apply strip method

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

    regularized_table_lake = {table['id']: table for table in regularized_source_dataset.tables}

    regularized_source_dataset._instances = [
        {
            'gold_table_id_set': sorted(
                instance['gold_table_id_set'],
                key=lambda x: regularized_table_lake[x]['metadata']
            ),
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
