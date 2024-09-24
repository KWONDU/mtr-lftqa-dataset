from tqdm.asyncio import tqdm_asyncio
from utils.format import table_format
from utils.openai import load_prompt, get_async_openai_response


async def generate_high_level_question(model_input, model_name):
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='generate_high_level_question'),
            user_prompt=load_prompt(
                role='user',
                task='generate_high_level_question'
            ).format(
                tables_info="\n".join(table_format(
                    table_num=idx+1,
                    metadata=table_info['metadata'],
                        header=table_info['header'],
                        cell=table_info['cell'],
                        serialize=True
                        ) for table_info in input_data['tables_info']
                    ),
                high_level_answer=input_data['high_level_answer']
            ),
            model_name=model_name
        ) for idx, input_data in enumerate(model_input)
    ]

    model_output_list = await tqdm_asyncio.gather(*tasks)

    generate_high_level_question_task_output = {
        'system_prompt': [model_output['system_prompt'] for model_output in model_output_list],
        'user_prompt': [model_output['user_prompt'] for model_output in model_output_list],
        'response': [model_output['response'] for model_output in model_output_list],
    }

    generated_question_list = [
        model_output['response'].strip()
        for model_output in model_output_list
    ]

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )
    
    return generate_high_level_question_task_output, generated_question_list, cost
