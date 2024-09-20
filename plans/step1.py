from tqdm.asyncio import tqdm_asyncio
from utils.format import data_format
from utils.openai import load_prompt, get_async_openai_response


async def generate_high_level_questions(model_input, model_name):
    
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='generate_high_level_questions'),
            user_prompt=load_prompt(
                role='user',
                task='generate_high_level_questions'
                ).format(
                    question_sql_extension_pairs="\n".join(data_format(
                        data_num=idx+1,
                        question=input_data['question'],
                        sql=input_data['sql'],
                        sub_table=input_data['sub_table'],
                        serialize=True
                    ) for idx, input_data in enumerate(input_data_list))
                ),
            model_name=model_name
        ) for input_data_list in model_input
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
