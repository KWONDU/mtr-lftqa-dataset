import asyncio
import json
import numpy as np
import pandas as pd
import random
import re
import traceback
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from utils.dataset import load_source_dataset
from utils.display import table_serialization, table_visualization
from utils.openai import get_async_openai_response, load_prompt, save_prompt
from get_shots import get_annotate_questions_task_shots


def clear_storage(storage_path):
    import glob
    import os

    storage_memory = glob.glob(os.path.join(storage_path, '*.txt'))
    for memory in storage_memory:
        try:
            os.remove(memory)
        except:
            continue
    
    return "[Done] clear storage."
    

async def annotate_questions_task(semaphore, model_input, model_name):
    tasks = [
        get_async_openai_response(
            semaphore=semaphore,
            system_prompt=load_prompt(role='system', task='annotate_questions'),
            user_prompt=load_prompt(role='user', task='annotate_questions').format(
                shots=input_data['shots'],
                gold_table_set_information="\n".join([
                    table_serialization(
                        table_num=tdx + 1,
                        metadata=gold_table['metadata'],
                        header=gold_table['header'],
                        cell=gold_table['cell']
                    )
                    for tdx, gold_table in enumerate(input_data['gold_table_set'])
                ]),
                nl_query_list="\n".join([
                    (
                        f"Query {ddx + 1} [Entail to table "
                        + ", ".join([
                            str(entailed_table_index + 1)
                            for entailed_table_index in entailed_table_indices
                        ])
                        + f"] {nl_query}"
                    )
                    for ddx, (entailed_table_indices, nl_query) in enumerate(zip(input_data['entailed_table_indices_list'], input_data['nl_query_list']))
                ])
            ),
            model_name=model_name,
            key=input_data['key']
        )
        for input_data in model_input
    ]

    model_output_list = []

    for task in tqdm_asyncio.as_completed(tasks, desc='Get OpenAI responses...'):
        model_output = await task
        model_output_list.append(model_output)
    
    cost = sum(
            [model_output['input_tokens_cost'] for model_output in model_output_list]
        ) + sum(
            [model_output['output_tokens_cost'] for model_output in model_output_list]
        )

    return sorted(model_output_list, key=lambda x: x['key']), cost


MODEL_NAME = 'gpt-3.5-turbo'


if __name__ == '__main__':
    ds = load_source_dataset(dataset_name='SourceWikipedia')

    with open('results/storage/table_document_set.json', 'r') as file:
        table_document_set = json.load(file)
    
    with open('results/storage/high_level_question_set.json', 'r') as file:
        high_level_question_set = json.load(file)

    table_lake = {tb['id']: tb for tb in ds.tables}

    random.seed(42)
    N = 1
    instance_set = random.sample(ds[:], N)

    high_level_qa_pair_set =  [
        {'gold_table_id_set': ins['gold_table_id_set'], 'annotation': [{'question': q, 'answer': ""} for q in ins['question_list']]}
        for ins in high_level_question_set
    ] # Output

    for role in ['system', 'user']:
        task = 'annotate_answer'
        save_prompt(file_path=f'prompts/{role}/{task}.txt', role=role, task=task)

    model_input = []
    for idx, instance in enumerate(instance_set):
        gold_table_set = [table_lake[t_id] for t_id in instance['gold_table_id_set']]

        gold_table_document_set = [
            ' '.join(ins['nl_document_list'])
            for ins in table_document_set
            if ins['table_id'] in instance['gold_table_id_set']
        ]
        

        data_list = instance['data_list']

        model_input.append({
            'gold_table_set': gold_table_set,
            'entailed_table_indices_list': [
                [
                    tdx
                    for tdx, gold_table in enumerate(gold_table_set)
                    if gold_table['id'] in data['entailed_table_id_set']
                ]
                for data in data_list
            ],
            'nl_query_list': [data['nl_query'] for data in data_list],
            'key': idx,
            'shots': get_annotate_questions_task_shots()
        })
    
    semaphore = asyncio.Semaphore(100)
    task_output_list, cost = asyncio.run(annotate_questions_task(
        semaphore=semaphore,
        model_input=model_input,
        model_name=MODEL_NAME
    ))

    ### CLEAR STORAGE ###
    print(clear_storage('storage_annotate_questions/results'))
    ### CLEAR STORAGE ###

    success_cnt, fail_cnt = 0, 0
    for task_output in tqdm(task_output_list, desc='Get high-level questions...'):
        idx = task_output['key']

        file_buffer = "# Gold table set information"
        for tdx, table_id in enumerate(instance_set[idx]['gold_table_id_set']):
            file_buffer = "\n".join([
                file_buffer,
                table_visualization(
                    table_num=tdx+1,
                    metadata=table_lake[table_id]['metadata'],
                    header=table_lake[table_id]['header'],
                    cell=table_lake[table_id]['cell']
                ),
                ""
            ])

        file_buffer = "\n".join([file_buffer, "# NL query list"])
        for ddx, data in enumerate(instance_set[idx]['data_list']):
            file_buffer = "\n".join([
                file_buffer,
                f"Query {ddx + 1} [Entail to table "
                + ", ".join([
                    str(table_index + 1)
                    for table_index, table_id in enumerate(instance_set[idx]['gold_table_id_set'])
                    if table_id in data['entailed_table_id_set']
                ])
                + f"] {data['nl_query']}"
            ])
        file_buffer = "\n".join([file_buffer, ""])

        try:
            indices = re.findall(r"Annotated question (\d+):", task_output['response'])
            questions = re.findall(r"Question: (.+?)(?=Difficulty:)", task_output['response'], re.DOTALL)
            difficulties = re.findall(r"Difficulty: (\w+)", task_output['response'])
            references = re.findall(r"Reference: \[(.*?)\]", task_output['response'])

            questions = [question.strip() for question in questions]
            references = [[int(ref) for ref in ref_list.split(", ")] if ref_list != '' else [] for ref_list in references]

            high_level_question_set[idx]['question_list'].extend(questions)
            for qdx, question, diff, ref in zip(indices, questions, difficulties, references):
                file_buffer = "\n".join([
                    file_buffer,
                    "\n".join([
                        f"# {int(qdx) + 1}th annotation:",
                        f"High-level question: {question}",
                        f"Annotation difficulty: {diff}",
                        f"Referred statement indices: {ref}",
                        ""
                    ])
                ])

            success_cnt += 1
            with open(f'storage_annotate_questions/results/{idx+1}_successed.txt', 'w') as file:
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
            with open(f'storage_annotate_questions/results/{idx+1}_failed.txt', 'w') as file:
                file.write(file_buffer)
    
    print("[Done] Annotate questions.")

    with open('storage_annotate_questions/high_level_question_set.json', 'w') as file:
        json.dump(high_level_question_set, file, indent=4)

    ### BUFFER ###
    with open('storage_annotate_questions/buffer.json', 'w') as file:
        json.dump(task_output_list, file, indent=4)
    ### BUFFER ###

    print(f"Cost: ${cost:.2f}")

    print(f"Success: {success_cnt}  |   Fail: {fail_cnt}")
