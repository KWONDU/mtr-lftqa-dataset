import os
from typing import Literal
from ._dir import current_dir


def save_prompt(file_path: str, role: Literal['assistant', 'system', 'user'], task: str) -> None:
    """Save given prompt

    [Params]
    file_path : str
    role      : Literal['assistant', 'system', 'user']
    task      : str
    """
    with open(file_path, 'r') as file:
        prompt = file.read()
    
    if not os.path.exists(f'{current_dir}/_prompt/{role}'):
        os.makedirs(f'{current_dir}/_prompt/{role}')
    
    with open(f'{current_dir}/_prompt/{role}/{task}.txt', 'w') as file:
        file.write(prompt)

    return None
