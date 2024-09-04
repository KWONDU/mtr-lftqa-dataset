import logging
from openai import OpenAI


def load_prompt_template(role):
    with open(f'prompt/{role}_prompt_template.txt', 'r') as file:
        return file.read()


def generate_long_form_answer(page_title, data, llm='gpt-3.5'):
    total_headers = [each_table['header'] for each_table in data]
    enum_total_headers = '\n'.join(f"table {i+1} header: {' | '.join(total_headers[i])}" for i in range(len(total_headers)))
    total_statements = [each_statement for each_table in data for each_statement in each_table['statements']]
    enum_total_statements = '\n'.join(f"{i+1}.\t{total_statements[i]}" for i in range(len(total_statements)))

    system_prompt = load_prompt_template('system').format(title=page_title, headers=enum_total_headers)
    user_prompt = load_prompt_template('user').format(statements=enum_total_statements)

    if llm == 'gpt-3.5':
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
    
    return system_prompt, user_prompt, response.choices[0].message.content


if __name__ == 'util_llm':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler())

    client = OpenAI(
        api_key="sk-qAXtWU2pnPEZXzx3wITQ4aMHFA7MVMMTKkKf6VlCGmT3BlbkFJQUPf6TcnLnda_5G7ScOEB5fp9GLyeYPJhZCcCPtU8A"
    )
