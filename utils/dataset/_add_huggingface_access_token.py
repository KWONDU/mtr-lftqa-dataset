import yaml
from ._dir import current_dir


def add_huggingface_access_token(token: str) -> None:
    """Add the given Huggingface access token

    [Param]
    token : str
    """
    with open(f'{current_dir}/_class/source/huggingface_access_token.yaml', 'w') as file:
        yaml.dump({'token': token}, file)
    
    return None
