import logging
from openai import OpenAI


def generate_long_form_answer(table, short_form_answers, llm='gpt-3.5'):
    enum_short_form_answers = ''.join(f"{i+1}.\t{short_form_answers[i]}\n" for i in range(len(short_form_answers)))
    template = (
        "Given a table metadata and a list of short-form statements which are related to the given table, generate a long-form statement that encompassing the overall flow of the given statements in a coherent, fluent, faithful and comprehensive manner.\n\n"
        f"Here are table metadata:\n"
        f"table_title:\t{None if table['title'] == '' else table['title']}\n"
        f"table_header:\t{' | '.join(table['cell'][0])}\n\n"
        f"Here are the short-form statements:\n{enum_short_form_answers}"
        "\nPlease generate a well-structured, long-form statement based on these table and short-form statements. Note that generated long-form statement should be a single sentence."
    )

    if llm == 'gpt-3.5':
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are tasked with identifying the overall flow of the given statements and grouping them under a common theme. Once you have recognized this theme, your goal is to generate a long-form statement that encompasses and unifies all the provided statements into a coherent, fluent, faithful and comprehensive answer. Ensure that you are an expert in this field."},
                {"role": "user", "content": template}
            ]
        )
    
    return response.choices[0].message.content


if __name__ == 'util_llm':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler())

    client = OpenAI(
        api_key="sk-qAXtWU2pnPEZXzx3wITQ4aMHFA7MVMMTKkKf6VlCGmT3BlbkFJQUPf6TcnLnda_5G7ScOEB5fp9GLyeYPJhZCcCPtU8A"
    )
