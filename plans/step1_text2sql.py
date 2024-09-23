from tqdm.asyncio import tqdm_asyncio
from utils.format import data_format, table_format
from utils.openai import load_prompt, get_async_openai_response


async def generate_high_level_questions(model_input, model_name):
    
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='generate_high_level_questions'),
            user_prompt=load_prompt(
                role='user',
                task='generate_high_level_questions'
                ).format(
                    tables_metadata_header_info="\n".join(table_format(
                        table_num=idx+1,
                        metadata=table_info['metadata'],
                        header=table_info['header'],
                        cell=None,
                        serialize=True
                        ) for idx, table_info in enumerate(input_data_dict['gold_table_set'])),
                    question_sql_query_extension_pairs="\n".join(data_format(
                        data_num=idx+1,
                        info_dict=input_data,
                        serialize=True
                    ) for idx, input_data in enumerate(input_data_dict['data_list']))
                ),
            model_name=model_name
        ) for input_data_dict in model_input
    ]
    
    model_output_list = await tqdm_asyncio.gather(*tasks)

    generate_high_level_questions_task_output = {
        'system_prompt': [model_output['system_prompt'] for model_output in model_output_list],
        'user_prompt': [model_output['user_prompt'] for model_output in model_output_list],
        'response': [model_output['response'] for model_output in model_output_list],
    }

    generated_questions_list = [
        ["".join(line.split(".")[1:]).strip() for line in model_output['response'].strip().split("\n")]
        for model_output in model_output_list
    ]

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )
    
    return generate_high_level_questions_task_output, generated_questions_list, cost
