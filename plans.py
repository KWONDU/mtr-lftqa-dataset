from tqdm.asyncio import tqdm_asyncio
from utils.display import table_serialization
from utils.openai import load_prompt, get_async_openai_response


async def annotate_questions_task(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_questions'),
            user_prompt=load_prompt(role='user', task='annotate_questions').format(
                shots=input_data['shots'],
                gold_table_set_information="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=gold_table['cell']
                    )
                    for tdx, gold_table in enumerate(input_data['gold_table_set'])
                ]),
                nl_query_list="\n".join([
                    (
                        f"Query {ddx + 1} [Entail to table "
                        + ", ".join(
                            [
                                str(idx + 1)
                                for idx, gold_table in enumerate(input_data['gold_table_set'])
                                if gold_table['id'] in data['entailed_table_id_set']
                            ]
                        )
                        + f"] {data['nl_query']}"
                    )
                    for ddx, data in enumerate(input_data['data_list'])
                ])
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for input_data in model_input
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


async def annotate_sub_answer_task(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_sub_answer'),
            user_prompt=load_prompt(role='user', task='annotate_sub_answer').format(
                shots=input_data['shots'],
                silver_table_information=table_serialization(
                    table_num=-1,
                    metadata=input_data['silver_table_information']['metadata'],
                    header=input_data['silver_table_information']['header'],
                    cell=input_data['silver_table_information']['cell']
                ),
                annotated_question=input_data['annotated_question']
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for input_data in model_input
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
