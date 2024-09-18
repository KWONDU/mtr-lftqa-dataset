from utils.util_format import table_format
from utils.util_llm import load_prompt, get_openai_response


def verify_and_modify_generated_question(idx, jdx, table_dict, gold_table_set, generated_question, llm_response_buffer):
    tables_metadata_header_info = "\n\n".join((table_format(
        table_num=table_idx+1,
        metadata=table_dict[gold_table_id]['metadata'],
        header=table_dict[gold_table_id]['header'],
        cell=None
    )) for table_idx, gold_table_id in enumerate(gold_table_set))

    system_prompt, user_prompt, response = get_openai_response(
        system_prompt=load_prompt(task='verify_and_modify_generated_question', role='system'),
        user_prompt=load_prompt(task='verify_and_modify_generated_question', role='user').format(
            tables_metadata_header_info=tables_metadata_header_info,
            generated_question=generated_question
            )
    )

    verification = response[response.find("Verification: ") + len("Verification: ") : response.find("Verification: ") + len("Verification: ") + 5].strip()
    high_level_question = response[response.find("Final question: ") + len("Final question: ") : -1].strip()

    llm_response_buffer['idx'].append(f"{idx+1}-{jdx+1}-verify_and_modify_generated_question\n")
    llm_response_buffer['system_prompt'].append(system_prompt)
    llm_response_buffer['user_prompt'].append(user_prompt)
    llm_response_buffer['response'].append(response)

    return verification, high_level_question, llm_response_buffer
