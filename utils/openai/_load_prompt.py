from typing import Literal
from ._dir import current_dir


def load_prompt(role: Literal['assistant', 'system', 'user'], task: str) -> str:
    """Load given prompt

    [Params]
    role : Literal['assistant', 'system', 'user']
    task : str

    [Return]
    prompt : str
    """
    with open(f'{current_dir}/_prompt/{role}/{task}.txt', 'r') as file:
        return file.read()
