import glob
import os


def clear_storage(storage_path: str, extension: str) -> None:
    """Clear storage with given extension

    [Params]
    storage_path : str
    extension    : str
    """
    storage_memory = glob.glob(os.path.join(storage_path, f'*.{extension}'))
    for memory in storage_memory:
        try:
            os.remove(memory)
        except:
            continue
    
    return None
