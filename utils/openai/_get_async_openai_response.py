import asyncio
from langchain.schema import AIMessage, HumanMessage, SystemMessage
from openai import OpenAIError
from ._load_llm import load_llm


async def get_async_openai_response(semaphore, system_prompt, user_prompt, model_name, key=None):
    llm = load_llm(model_name=model_name)

    if model_name == 'gpt-3.5-turbo':
        input_tokens_price, output_tokens_price = 1.50/1e6, 2.00/1e6

    is_16k = False

    async with semaphore:
        while True:
            try:
                system_message = SystemMessage(content=system_prompt)
                human_message = HumanMessage(content=user_prompt)

                response = await llm.ainvoke([system_message, human_message])

                input_tokens = response.usage_metadata['input_tokens']
                output_tokens = response.usage_metadata['output_tokens']

                input_tokens_cost = input_tokens * input_tokens_price
                output_tokens_cost = output_tokens * output_tokens_price

                break

            except OpenAIError as e:
                if "maximum context length" in str(e) and not is_16k:
                    llm = load_llm(model_name=f'{model_name}-16k')
                    input_tokens_price, output_tokens_price = input_tokens_price * 2, output_tokens_price * 2
                    is_16k = True
                else:
                    print(f'{key}: [OpenAIError]\t{e}')
                    return {
                        'system_prompt': system_prompt,
                        'user_prompt': user_prompt,
                        'response': None,
                        'input_tokens_cost': 0,
                        'output_tokens_cost': 0,
                        'key': key
                    }
            
            except Exception as e:
                print(f'{key}: [Exception]\t{e}')
                return {
                        'system_prompt': system_prompt,
                        'user_prompt': user_prompt,
                        'response': None,
                        'input_tokens_cost': 0,
                        'output_tokens_cost': 0,
                        'key': key
                    }
    
    return {
        'system_prompt': system_prompt,
        'user_prompt': user_prompt,
        'response': response.content,
        'input_tokens_cost': input_tokens_cost,
        'output_tokens_cost': output_tokens_cost,
        'key': key
    }
