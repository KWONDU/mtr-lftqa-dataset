import yaml
from ._dir import current_dir


def _load_openai_api_key():
    try:
        with open(f'{current_dir}/openai_api_key.yaml', 'r') as file:
            return yaml.load(file, Loader=yaml.FullLoader)['api_key']
    except Exception as e:
        return None
