import dill
import glob
from typing import Literal, Union
from ._dir import current_dir


def load_source_dataset(dataset_name: Literal['SourceMultiTabQA', 'SourceOpenWikiTable']) -> Union[object, None]:
    """Load source dataset

    [Param]
    dataset_name: Literal['SourceMultiTabQA', 'SourceOpenWikiTable'])

    [Return]
    dataset : Union[object, None]
    """
    processed_dataset_name = dataset_name.replace('-', '').lower()
    if processed_dataset_name.find('source') < 0:
        return None

    try:
        serialized_data = b''
        file_path_list = glob.glob(f'{current_dir}/_dump_source/{processed_dataset_name}*.pkl')
        
        for file_path in sorted(file_path_list):
            with open(file_path, 'rb') as file:
                serialized_data = serialized_data + file.read()
        
        dataset = dill.loads(serialized_data)

    except Exception as _:
        return None
    
    return dataset
