import os
from ._dir import current_dir


def view_prompt_list():
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
