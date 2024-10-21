import os
from typing import Literal
from ._dir import current_dir


def remove_prompt(role: Literal['assistant', 'system', 'user'], task: str) -> bool:
    """Remove given prompt

    [Params]
    role : Literal['assistant', 'system', 'user']
    task : str

    [Return]
    success: bool
    """
    file_path = f'{current_dir}/_prompt/{role}/{task}.txt'

    if os.path.exists(file_path):
        os.remove(file_path)
        return True

    return False
