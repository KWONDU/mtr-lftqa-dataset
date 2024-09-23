import os
from ._dir import current_dir


def remove_prompt(role, task):
    file_path = f'{current_dir}/_prompt/{role}/{task}.txt'

    if os.path.exists(file_path):
        os.remove(file_path)
        return True

    return False
