import asyncio
import json
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from typing import Any, Dict, List, Tuple
    

async def annotate_answer_task(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_answer_with_high_header_sim'),
            user_prompt=load_prompt(role='user', task='annotate_answer_with_high_header_sim').format(
                gold_table_document_set="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=None
                    )
                    + f" [document]: {nl_document}" if nl_document is not None else ""
                    for tdx, (gold_table, nl_document) in enumerate(input_data['gold_table_document_set'])
                ]),
                question=input_data['question']
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for input_data in model_input
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks, desc=f"[{'OpenAI':<7}]"):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


def annotate_answer(
        table_lake: Dict[str, Dict[str, Any]],
        model_name: str,
        semaphore: asyncio.Semaphore
    ) -> Tuple[List[Dict[str, Any]], int, int, int]:
    """Task: annotate answer

    [Params]
    table_lake   : Dict[str, Dict[str, Any]]
    load_shot    : object
    model_name   : str
    semaphore    : asyncio.Semaphore

    [Returns]
    high_level_qa_pair_set : List[Dict[str, Any]]
    success_cnt             : int
    fail_cnt                : int
    cost                    : int
    """
    # Initialization and setup
    with open('results/storage/high_header_sim/table_document_set.json', 'r') as file:
        table_document_set = json.load(file)
    
    with open('results/storage/high_header_sim/high_level_question_set.json', 'r') as file:
        high_level_question_set = json.load(file)

    high_level_qa_pair_set =  [
        {
            'gold_table_id_set': instance['gold_table_id_set'],
            'annotation': [
                {
                    'question': question,
                    'answer': ""
                }
                for question in instance['question_list']
            ]
        }
        for instance in high_level_question_set
    ] # Output

    for role in ['system', 'user']:
        task = 'annotate_answer_with_high_header_sim'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    # Main task
    model_input = []
    for idx, instance in enumerate(high_level_question_set):
        gold_table_document_set = [
            (
                table_lake[t_id],
                next((" ".join(tb_doc['nl_document_list']) for tb_doc in table_document_set if tb_doc['table_id'] == t_id), None) # no entailed (q, s)
            )
            for t_id in instance['gold_table_id_set']
        ]

        for jdx, question in enumerate(instance['question_list']):
            model_input.append({
                'gold_table_document_set': gold_table_document_set,
                'question': question,
                'key': (idx, jdx)
            })
    
    semaphore = asyncio.Semaphore(100)
    task_output_list, cost = asyncio.run(annotate_answer_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=model_name
    ))

    # Clear
    clear_storage(storage_path='buffer/high_header_sim/annotate_answer', extension='txt')

    # Storage
    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc=f"[{'Storage':<7}]"):
        idx, jdx = task_output['key']
        question = high_level_question_set[idx]['question_list'][jdx]

        file_buffer = "# Gold table document set"
        for tdx, table_id in enumerate(high_level_question_set[idx]['gold_table_id_set']):
            file_buffer = "\n".join([
                file_buffer,
                table_visualization(
                    table_num=tdx+1,
                    metadata=table_lake[table_id]['metadata'],
                    header=table_lake[table_id]['header'],
                    cell=table_lake[table_id]['cell']
                ),
                "NL document list:",
                "\n".join(next((
                    tb_doc['nl_document_list']
                    for tb_doc in table_document_set
                    if tb_doc['table_id'] == table_id
                ), "")), # no entailed (q, s)
                ""
            ])
        
        file_buffer = "\n".join([
            file_buffer,
            f"# Question: {question}",
            ""
        ])

        try:
            answer = task_output['response']

            high_level_qa_pair_set[idx]['annotation'][jdx]['answer'] = answer
            file_buffer = "\n".join([
                file_buffer,
                f"# Answer: {answer}",
                ""
            ])

            success_cnt += 1
            with open(f'buffer/high_header_sim/annotate_answer/{idx+1}_{jdx+1}_successed.txt', 'w') as file:
                file.write(file_buffer)
        
        except:
            file_buffer = "\n".join([
                file_buffer,
                "# Response",
                task_output['response'],
                "",
                "# Exception",
                traceback.format_exc(),
                ""
            ])

            fail_cnt += 1
            with open(f'buffer/high_header_sim/annotate_answer/{idx+1}_{jdx+1}_failed.txt', 'w') as file:
                file.write(file_buffer)

    # Buffer
    with open('buffer/high_header_sim/annotate_answer.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)

    return high_level_qa_pair_set, success_cnt, fail_cnt, cost


if __name__ == 'steps.high_header_sim.annotate_answer':
    from utils.display import clear_storage, table_serialization, table_visualization
    from utils.openai import get_async_openai_response, load_prompt, save_prompt
