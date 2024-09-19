from ._dir import current_dir


def load_prompt(role, task):
    with open(f'{current_dir}/_prompt/{role}/{task}.txt', 'r') as file:
        return file.read()
