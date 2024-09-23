import dill
from ._dir import current_dir


def save_source_dataset(dataset, dataset_name):
    chunk_size = 50_000_000
    # mb_size = chunk_size / pow(2, 30)

    processed_dataset_name = dataset_name.replace('-', '').lower()
    if processed_dataset_name.find('source') < 0:
        return None

    serialized_data = dill.dumps(dataset)

    if len(serialized_data) <= chunk_size:
        with open(f'{current_dir}/_dump_source/{processed_dataset_name}.pkl', 'wb') as file:
            file.write(serialized_data)
        return [f'{processed_dataset_name}.pkl']
    else:
        for i in range(0, len(serialized_data), chunk_size):
            chunk = serialized_data[i : i + chunk_size]
            with open(f'{current_dir}/_dump_source/{processed_dataset_name}-{i // chunk_size + 1}.pkl', 'wb') as file:
                file.write(chunk)
        return [
            f'{processed_dataset_name}-{i}.pkl'
            for i in range(0, len(serialized_data), chunk_size)
            ]
