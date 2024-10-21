import dill
import glob
from typing import Union
from ._dir import current_dir


def load_dataset(dataset_name: str) -> Union[object, None]:
    """Load dataset

    [Param]
    dataset_name: str

    [Return]
    dataset : Union[object, None]
    """
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
