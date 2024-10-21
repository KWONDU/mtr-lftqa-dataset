from collections import defaultdict
from langchain_openai import ChatOpenAI
from typing import Literal
from ._load_openai_api_key import _load_openai_api_key


def load_llm(model_name: Literal['gpt-3.5-turbo', 'gpt-3.5-turbo-16k']) -> ChatOpenAI:
    """Load OpenAI LLM model

    [Param]
    model_name: Literal['gpt-3.5-turbo', 'gpt-3.5-turbo-16k']

    [Return]
    llm : ChatOpenAI
    """
    if model_name not in llm_buffer:
        llm_buffer[model_name] = ChatOpenAI(
                temperature=0,
                model=model_name,
                api_key=api_key
            )
    
    return llm_buffer[model_name]


if __name__ == 'utils.openai._load_llm':
    api_key = _load_openai_api_key()
    llm_buffer = defaultdict()
