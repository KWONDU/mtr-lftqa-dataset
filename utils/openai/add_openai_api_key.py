import yaml
from ._dir import current_dir


def add_openai_api_key(api_key):
    with open(f'{current_dir}/openai_api_key.yaml', 'w') as file:
        yaml.dump({'api_key': api_key}, file)
    
    return api_key
