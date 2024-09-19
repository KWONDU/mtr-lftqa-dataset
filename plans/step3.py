from utils.format import table_format
from utils.openai import load_prompt, get_openai_response


def generate_high_level_answer(idx, jdx, table_dict, gold_table_set, high_level_question, llm_response_buffer):
    tables_info = "\n".join((table_format(
        table_num=table_idx+1,
        metadata=table_dict[gold_table_id]['metadata'],
        header=table_dict[gold_table_id]['header'],
        cell=table_dict[gold_table_id]['cell']
    )) for table_idx, gold_table_id in enumerate(gold_table_set))

    system_prompt, user_prompt, response = get_openai_response(
        system_prompt=load_prompt(task='generate_high_level_answer', role='system'),
        user_prompt=load_prompt(task='generate_high_level_answer', role='user').format(
            tables_info=tables_info,
            high_level_question=high_level_question
            )
    )

    high_level_answer = response.strip()
    
    llm_response_buffer['idx'].append(f"{idx+1}-{jdx+1}-generate_high_level_answer\n")
    llm_response_buffer['system_prompt'].append(system_prompt)
    llm_response_buffer['user_prompt'].append(user_prompt)
    llm_response_buffer['response'].append(response)

    return high_level_answer, llm_response_buffer
