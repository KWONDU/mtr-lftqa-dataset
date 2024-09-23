from tqdm.asyncio import tqdm_asyncio
from utils.format import table_format
from utils.openai import load_prompt, get_async_openai_response


async def filter_each_generated_question(model_input, model_name):
    None

async def ___(model_input, model_name):
    tasks = [
        get_async_openai_response(
            system_prompt=load_prompt(role='system', task='verify_and_modify_each_generated_question'),
            user_prompt=load_prompt(
                role='user',
                task='verify_and_modify_each_generated_question'
                ).format(
                    tables_metadata_header_info="\n".join(table_format(
                        table_num=idx+1,
                        metadata=table_info['metadata'],
                        header=table_info['header'],
                        cell=None,
                        serialize=True
                        ) for table_info in input_data['tables_info']
                    ),
                    generated_question=input_data['generated_question']
                ),
            model_name=model_name
        ) for idx, input_data in enumerate(model_input)
    ]

    model_output_list = await tqdm_asyncio.gather(*tasks)

    verify_and_modify_each_generated_question_task_output = {
        'system_prompt': [model_output['system_prompt'] for model_output in model_output_list],
        'user_prompt': [model_output['user_prompt'] for model_output in model_output_list],
        'response': [model_output['response'] for model_output in model_output_list],
    }

    verification_list = [
        model_output['response'][
            model_output['response'].find("Verification: ") + len("Verification: ")
            :
            model_output['response'].find("Verification: ") + len("Verification: ") + 5
            ].strip()
        for model_output in model_output_list
    ]

    modified_each_question_list = [
        model_output['response'][
            model_output['response'].find("Final question: ")+len("Final question: ")
            :
            ].strip()
        for model_output in model_output_list
    ]

    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )
    
    return verify_and_modify_each_generated_question_task_output, verification_list, modified_each_question_list, cost
