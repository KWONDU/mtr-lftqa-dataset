import yaml
from ._dir import current_dir


def add_huggingface_access_token(token):
    with open(f'{current_dir}/_class/source/huggingface_access_token.yaml', 'w') as file:
        yaml.dump({'token': token}, file)
    
    return token
