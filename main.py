import argparse
import logging
import random
from collections import defaultdict
from tqdm import tqdm
from utils.dataset import load_dataset
from utils.openai import save_prompt

from plans.step0 import display_data, display_table
from plans.step1 import generate_high_level_questions
from plans.step2 import verify_and_modify_generated_question
from plans.step3 import generate_high_level_answer


def extract_multi_table_data_pairs(table_lake, dataset):
    # 1. Transform table lake to make it easier to use: table_dict
    table_dict = {table['id']: table for table in table_lake}

    # 2. Group by same gold table set (data): data_dict
    data_dict = defaultdict(list)
    for data in dataset:
        data_dict[tuple(data['gold_tables'])].append(data)

    # 3. Remove pair with single table set or single data
    keys_to_delete = []
    for table_key, data_value in data_dict.items():
        if len(table_key) == 1 or len(data_value) == 1:
            keys_to_delete.append(table_key)
    for table_key in keys_to_delete:
        del data_dict[table_key]

    return table_dict, data_dict


def main(dataset_name, sample_n):
    dataset = load_dataset(dataset_name=dataset_name)
    logger.info(dataset)

    # Extract Spider sub-dataset
    table_lake = [table for table in dataset.tables if table['source'] == 'Spider']
    spider_subset = [data for data in dataset[:]
                     if all(gold_table_id in {table['id'] for table in table_lake}
                            for gold_table_id in data['gold_tables'])
                    ]

    # Extract gold multi-table set and corresponding multi data pairs
    table_dict, data_dict = extract_multi_table_data_pairs(table_lake=table_lake, dataset=spider_subset)

    # Initialize LLM response buffer
    llm_response_buffer = defaultdict(list)

    # save prompt
    for role in ['system', 'user']:
        for task in ['generate_high_level_questions', 'verify_and_modify_generated_question', 'generate_high_level_answer']:
            save_prompt(f'prompt/{role}/{task}.txt', role, task)

    # Sample data
    random.seed(42) # fix sampled data
    sampled_data_dict = dict(random.sample(list(data_dict.items()), sample_n))

    for idx, (gold_table_set, data_list) in tqdm(enumerate(sampled_data_dict.items()), desc="sampled data", total=sample_n):
        # For each pair,
        # 1. Display information of gold table set and data list
        display_info = ">> Display information of gold table set and data list."

        display_info = "\n".join([display_info, "[Gold table set information]"])
        display_info = "\n".join([display_info, display_table(table_dict=table_dict, gold_table_set=gold_table_set)])
        display_info += "\n"

        display_info = "\n".join([display_info, "[Data list information]"])
        display_info = "\n".join([display_info, display_data(data_list=data_list)])
        display_info += "\n"

        tqdm.write(f"[Done #{idx + 1}] Display information of gold table set and data list.")

        # 2. Generate high-level questions using data['question'] and data['answer']['SQL']
        generate_high_level_questions_result = ">> Generate high-level questions using data['question'] and data['answer']['SQL']."

        response, llm_response_buffer = generate_high_level_questions(
            idx=idx,
            data_list=data_list,
            llm_response_buffer=llm_response_buffer
        )

        generate_high_level_questions_result = "\n".join([generate_high_level_questions_result, f"[Generated high-level questions]\n{response}"])

        tqdm.write(f"[Done #{idx + 1}] Generate high-level questions using data['question'] and data['answer']['SQL'].")

        # For each generated question,
        annotate_questions_and_answers_result = ""
        generated_questions = [question.split(". ")[1] for question in response.split("\n")]
        for jdx, generated_question in tqdm(enumerate(generated_questions), desc="generated questions", total=len(generated_questions)):
            # 3. Verify and modify generated question using table['metadata'] and table['header']
            verify_and_modify_generated_each_question_result = ">> Verify and modify generated question using table['metadata'] and table['header']."

            verification, high_level_question, llm_response_buffer = verify_and_modify_generated_question(
                idx=idx,
                jdx=jdx,
                table_dict=table_dict,
                gold_table_set=gold_table_set,
                generated_question=generated_question,
                llm_response_buffer=llm_response_buffer
            )

            verify_and_modify_generated_each_question_result = "\n".join([verify_and_modify_generated_each_question_result, f"[Annotated question]\n({verification}) {high_level_question}"])

            tqdm.write(f"[Done #{idx + 1}-{jdx + 1}] Verify and modify generated question using table['metadata'] and table['header'].")

            # 4. Generate high-level answer using modifed question and gold table set information
            generate_each_high_level_answer_result = ">> Generate high-level answer using modifed question and gold table set information."

            try:
                high_level_answer, llm_response_buffer = generate_high_level_answer(
                    idx=idx,
                    jdx=jdx,
                    table_dict=table_dict,
                    gold_table_set=gold_table_set,
                    high_level_question=high_level_question,
                    llm_response_buffer=llm_response_buffer
                )
            except Exception as e:
                logger.warning(f"[Error #{idx + 1}-{jdx + 1}] {e}")
                continue

            generate_each_high_level_answer_result = "\n".join([generate_each_high_level_answer_result, f"[Annotated answer]\n{high_level_answer}"])

            tqdm.write(f"[Done #{idx + 1}-{jdx + 1}] Generate high-level answer using modifed question and gold table set information.")

            annotate_questions_and_answers_result = "\n".join([annotate_questions_and_answers_result, f"> About generated question #{jdx + 1} . . ."])
            annotate_questions_and_answers_result = "\n".join([annotate_questions_and_answers_result, f"[Generated question]\n{generated_question}"])
            annotate_questions_and_answers_result += "\n"
            annotate_questions_and_answers_result = "\n".join([annotate_questions_and_answers_result, f"{verify_and_modify_generated_each_question_result}"])
            annotate_questions_and_answers_result += "\n"
            annotate_questions_and_answers_result = "\n".join([annotate_questions_and_answers_result, f"{generate_each_high_level_answer_result}"])
            annotate_questions_and_answers_result += "\n"

        with open(f'results/annotation_result_{idx + 1}.txt', 'w') as file:
            file.write(display_info)
            file.write("\n")
            file.write(generate_high_level_questions_result)
            file.write("\n")
            file.write(annotate_questions_and_answers_result)
    
    with open(f'results/llm_responses.txt', 'w') as file:
        for idx_buffer, system_prompt_buffer, user_prompt_buffer, response_buffer in \
                zip(llm_response_buffer['idx'], llm_response_buffer['system_prompt'], llm_response_buffer['user_prompt'], llm_response_buffer['response']):
            file.write(f"[Idx]: {idx_buffer}")
            file.write(f"[System prompt]\n{system_prompt_buffer}")
            file.write("\n")
            file.write(f"[User prompt]\n{user_prompt_buffer}")
            file.write("\n")
            file.write(f"[Response]\n{response_buffer}")
            file.write("\n\n==\n\n")


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default='MultiTabQA', help='dataset name')
    parser.add_argument('-n', type=int, default=1, help='number of sampled data')
    parser.add_argument('-k', type=bool, default=True, help='whether add openai api key')

    args, _ = parser.parse_known_args()
    logger.info(args)

    if not args.k:
        your_api_key = 'insert_your_openai_api_key'
        from utils.openai import add_openai_api_key
        add_openai_api_key(api_key=your_api_key)

    main(
        dataset_name = args.d,
        sample_n = args.n
    )
