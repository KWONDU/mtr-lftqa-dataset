import logging
from openai import OpenAI


def load_prompt(task, role):
    with open(f'prompt/{role}/{task}.txt', 'r') as file:
        return file.read()


def get_openai_response(system_prompt, user_prompt, llm='gpt-3.5'):
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
