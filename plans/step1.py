from utils.format import data_format
from utils.openai import load_prompt, get_openai_response


def generate_high_level_questions(idx, data_list, llm_response_buffer):
    question_sql_pairs = "\n\n".join(data_format(
        data_num=data_idx+1,
        question=data['question'],
        sql=next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type=='SQL'),
        sub_table=None
    ) for data_idx, data in enumerate(data_list))

    system_prompt, user_prompt, response = get_openai_response(
        system_prompt=load_prompt(task='generate_high_level_questions', role='system'),
        user_prompt=load_prompt(task='generate_high_level_questions', role='user').format(
            question_sql_pairs=question_sql_pairs
            )
    )

    llm_response_buffer['idx'].append(f"{idx+1}-generate_high_level_questions\n")
    llm_response_buffer['system_prompt'].append(system_prompt)
    llm_response_buffer['user_prompt'].append(user_prompt)
    llm_response_buffer['response'].append(response)

    return response, llm_response_buffer
