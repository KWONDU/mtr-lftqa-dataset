import dill
import importlib
import os
import sys


def load_large_object(files_path_list, sub_folder_dir, module_name):
    sys.path.append(os.path.join(os.path.dirname(__file__), sub_folder_dir))
    __module = importlib.import_module(module_name)

    serialized_data = b''

    for file_path in files_path_list:
        with open(file_path, 'rb') as file:
            serialized_data = serialized_data + file.read()

    return dill.loads(serialized_data)


def save_large_object(obj, file_path_prefix):
    chunk_size = 50_000_000
    # mb_size = chunk_size / pow(2, 30)

    serialized_data = dill.dumps(obj)

    if len(serialized_data) <= chunk_size:
        with open(f'{file_path_prefix}.pkl', 'wb') as file:
            file.write(serialized_data)
        return [f'{file_path_prefix}.pkl']
    else:
        for i in range(0, len(serialized_data), chunk_size):
            chunk = serialized_data[i : i + chunk_size]
            with open(f'{file_path_prefix}-{i // chunk_size + 1}.pkl', 'wb') as file:
                file.write(chunk)
        return [
            f'{file_path_prefix}-{i}.pkl'
            for i in range(0, len(serialized_data), chunk_size)
            ]
