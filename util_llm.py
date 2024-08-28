import logging
from openai import OpenAI


def short2long(questions, llm='gpt-3.5'):
    template = (
        "Given short-form questions, you have to generate long-form question with high faithfulness and comprehensiveness.\n\n"
        f"These are short-form queries: {questions}\n"
        f"Please generate long-form question. you only output a single long-form question with a single sentence. Let's think step by step."
    )

    if llm == 'gpt-3.5':
        response = client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": "You are a helpful assistant."},
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
