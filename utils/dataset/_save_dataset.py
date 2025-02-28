import dill
from typing import List
from ._dir import current_dir


def save_dataset(dataset: object, dataset_name: str) -> List[str]:
    """Save dataset

    [Params]
    dataset      : object
    dataset_name : str

    [Return]
    file_path_list : List[str]
    """
    chunk_size = 50_000_000
    # mb_size = chunk_size / pow(2, 30)

    processed_dataset_name = dataset_name.replace('-', '').lower()

    serialized_data = dill.dumps(dataset)

    if len(serialized_data) <= chunk_size:
        with open(f'{current_dir}/_dump/{processed_dataset_name}.pkl', 'wb') as file:
            file.write(serialized_data)
        return [f'{processed_dataset_name}.pkl']
    else:
        for i in range(0, len(serialized_data), chunk_size):
            chunk = serialized_data[i : i + chunk_size]
            with open(f'{current_dir}/_dump/{processed_dataset_name}-{i // chunk_size + 1}.pkl', 'wb') as file:
                file.write(chunk)
        return [
            f'{processed_dataset_name}-{i}.pkl'
            for i in range(0, len(serialized_data), chunk_size)
            ]
