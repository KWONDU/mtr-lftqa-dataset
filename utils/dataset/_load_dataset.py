import dill
import glob
from ._dir import current_dir


def load_dataset(dataset_name):
    processed_dataset_name = dataset_name.replace('-', '').lower()

    try:
        serialized_data = b''
        file_path_list = glob.glob(f'{current_dir}/_dump/{processed_dataset_name}*.pkl')
        
        for file_path in sorted(file_path_list):
            with open(file_path, 'rb') as file:
                serialized_data = serialized_data + file.read()
        
        dataset = dill.loads(serialized_data)

    except Exception as _:
        return None
    
    return dataset
