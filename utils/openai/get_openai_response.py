from ._client import client


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
