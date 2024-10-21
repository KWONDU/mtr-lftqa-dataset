import os
from typing import Dict, List
from ._dir import current_dir


def view_prompt_list() -> Dict[str, List[str]]:
    """Get all stored prompt list

    e.g.
    {asssitant: [task1, task2, ...], ...}

    [Return]
    prompt_list : Dict[str, List[str]]
    """
    prompt_list = {}

    for path, roles, tasks in os.walk(f'{current_dir}/_prompt'):
        if roles:
            for role in roles:
                prompt_list[role] = []
        elif tasks:
            role = path.split('/')[-1]
            for task in tasks:
                prompt_list[role].append(task.replace('_', ' ').strip('.')[0])
    
    return prompt_list
