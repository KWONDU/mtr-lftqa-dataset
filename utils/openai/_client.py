import yaml
from openai import OpenAI
from ._dir import current_dir


with open(f'{current_dir}/openai_api_key.yaml', 'r') as file:
    api_key = yaml.load(file, Loader=yaml.FullLoader)['api_key']

client = OpenAI(
    api_key=api_key
)
