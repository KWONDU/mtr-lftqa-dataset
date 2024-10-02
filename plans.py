from tqdm.asyncio import tqdm_asyncio
from utils.display import table_serialization
from utils.openai import load_prompt, get_async_openai_response


async def generate_document(semaphore, model_input, model_name): # gold_table_set_info, statement_list
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='generate_document'),
            user_prompt=load_prompt(role='user', task='generate_document').format(
                gold_table_set_info="\n".join([
                    table_serialization(
                        table_num=tdx+1,
                        metadata=table['metadata'],
                        header=table['header'],
                        cell=table['cell']
                    )
                    for tdx, table in enumerate(input_data['gold_table_set'])
                ]),
                statement_list=" ".join([
                    statement
                    for _, statement in enumerate(input_data['statement_list'])
                ])
            ),
            model_name=model_name,
            key=num
        )
        for num, input_data in enumerate(model_input)
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


async def annotate_answer(semaphore, model_input, model_name): # document
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_answer'),
            user_prompt=load_prompt(role='user', task='annotate_answer').format(
                document=input_data['document']
            ),
            model_name=model_name,
            key=num
        )
        for num, input_data in enumerate(model_input)
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


async def annotate_question(semaphore, model_input, model_name): # gold_table_set_info, answer
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_question'),
            user_prompt=load_prompt(role='user', task='annotate_question').format(
                gold_table_set_info="\n".join([
                    table_serialization(
                        table_num=tdx+1,
                        metadata=table['metadata'],
                        header=table['header'],
                        cell=table['cell']
                    )
                    for tdx, table in enumerate(input_data['gold_table_set'])
                ]),
                answer=input_data['answer']
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for _, input_data in enumerate(model_input)
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost
