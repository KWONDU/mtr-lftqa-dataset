from tqdm.asyncio import tqdm_asyncio
from utils.format import data_format, table_format
from utils.openai import load_prompt, get_async_openai_response


async def generate_document(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='generate_document'),
            user_prompt=load_prompt(role='user', task='generate_document').format(
                gold_table_set_info="\n\n".join([
                    table_format(
                        table_num=tdx+1,
                        metadata=table['metadata'],
                        header=table['header'],
                        cell=table['cell'],
                        serialize=True
                    )
                    for tdx, table in enumerate(input_data['gold_table_set'])
                ]),
                sentence_list="\n\n".join([
                    data_format(
                        data_num=ddx+1,
                        data=sentence,
                        type='sentence'
                    )
                    for ddx, sentence in enumerate(input_data['sentence_list'])
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


async def annotate_answer(semaphore, model_input, model_name):
    None


async def annotate_question(semaphore, model_input, model_name):
    None
