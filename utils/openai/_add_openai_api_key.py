import yaml
from ._dir import current_dir


def add_openai_api_key(api_key: str) -> None:
    """Add the given OpenAI api key

    [Param]
    api_key : str
    """
    with open(f'{current_dir}/openai_api_key.yaml', 'w') as file:
        yaml.dump({'api_key': api_key}, file)
    
    return None
