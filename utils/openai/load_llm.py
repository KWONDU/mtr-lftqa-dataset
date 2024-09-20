import yaml
from collections import defaultdict
from langchain_openai import ChatOpenAI
from ._load_openai_api_key import _load_openai_api_key


def load_llm(model_name):
    if model_name not in llm_buffer:
        llm_buffer[model_name] = ChatOpenAI(
                temperature=0,
                model=model_name,
                api_key=api_key
            )
    
    return llm_buffer[model_name]


if __name__ == 'utils.openai.load_llm':
    api_key = _load_openai_api_key()
    llm_buffer = defaultdict()
