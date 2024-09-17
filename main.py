import logging
import random
from collections import defaultdict
from tqdm import tqdm
from util_format import data_format, table_format
from util_llm import load_prompt, get_openai_response
from util import load_dataset, parser


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

    # Sample data
    random.seed(42) # fix sampled data
    sampled_data_dict = dict(random.sample(list(data_dict.items()), sample_n))

    for idx, (gold_table_set, data_list) in tqdm(enumerate(sampled_data_dict.items()), desc="sampled data", total=sample_n):
        # For each pair,
        # 1. Display information of gold table set and data list
        display_info = ">> Display information of gold table set and data list.\n"

        display_info += "[Gold table set information]\n"
        display_info += "".join(table_format(
            table_num=table_idx+1,
            metadata=table_dict[gold_table_id]['metadata'],
            header=table_dict[gold_table_id]['header'],
            cell=table_dict[gold_table_id]['cell']
        ) for table_idx, gold_table_id in enumerate(gold_table_set))

        display_info += "[Data list information]\n"
        display_info += "".join(data_format(
            data_num=data_idx+1,
            question=data['question'],
            sql=data['answer'][0],
            sub_table=data['answer'][1]
        ) for data_idx, data in enumerate(data_list))

        tqdm.write(f"[Done #{idx + 1}] Display information of gold table set and data list.")

        # Initialize LLM response buffer
        llm_response_buffer = defaultdict(list)

        # 2. Generate high-level questions using data['question'] and data['answer']['SQL']
        generate_high_level_questions_result = ">> Generate high-level questions using data['question'] and data['answer']['SQL'].\n"

        question_sql_pairs = "".join(data_format(
            data_num=data_idx+1,
            question=data['question'],
            sql=data['answer'][0],
            sub_table=None
        ) for data_idx, data in enumerate(data_list))

        system_prompt, user_prompt, response = get_openai_response(
            system_prompt=load_prompt(task='generate_high_level_questions', role='system'),
            user_prompt=load_prompt(task='generate_high_level_questions', role='user').format(
                question_sql_pairs=question_sql_pairs
                )
        )

        generate_high_level_questions_result += f"[Generated high-level questions]\n{response}\n"
        llm_response_buffer['system_prompt'].append(system_prompt)
        llm_response_buffer['user_prompt'].append(user_prompt)
        llm_response_buffer['response'].append(response)

        tqdm.write(f"[Done #{idx + 1}] Generate high-level questions using data['question'] and data['answer']['SQL'].")

        # For each generated question,
        annotate_questions_and_answers_result = ""

        tables_metadata_header_info = "".join((table_format(
            table_num=table_idx+1,
            metadata=table_dict[gold_table_id]['metadata'],
            header=table_dict[gold_table_id]['header'],
            cell=None
        )) for table_idx, gold_table_id in enumerate(gold_table_set))
        tables_info = "".join((table_format(
            table_num=table_idx+1,
            metadata=table_dict[gold_table_id]['metadata'],
            header=table_dict[gold_table_id]['header'],
            cell=table_dict[gold_table_id]['cell']
        )) for table_idx, gold_table_id in enumerate(gold_table_set))

        generated_questions = [question.split(". ")[1] for question in response.split("\n")]
        for jdx, generated_question in tqdm(enumerate(generated_questions), desc="generated questions", total=len(generated_questions)):
            # 3. Verify and modify generated question using table['metadata'] and table['header']
            verify_and_modify_generated_each_question_result = "\n>> Verify and modify generated question using table['metadata'] and table['header'].\n"

            system_prompt, user_prompt, response = get_openai_response(
                system_prompt=load_prompt(task='verify_and_modify_generated_question', role='system'),
                user_prompt=load_prompt(task='verify_and_modify_generated_question', role='user').format(
                    tables_metadata_header_info=tables_metadata_header_info,
                    generated_question=generated_question
                    )
            )

            verify_and_modify_generated_each_question_result += f"[Annotated question]\n{response}\n"
            llm_response_buffer['system_prompt'].append(system_prompt)
            llm_response_buffer['user_prompt'].append(user_prompt)
            llm_response_buffer['response'].append(response)

            tqdm.write(f"[Done #{idx + 1}-{jdx + 1}] Verify and modify generated question using table['metadata'] and table['header'].")

            # 4. Generate high-level answer using modifed question and gold table set information
            generate_each_high_level_answer_result = "\n>> Generate high-level answer using modifed question and gold table set information.\n"

            high_level_question = response[response.find("Final question: ")+1:-1]

            try:
                system_prompt, user_prompt, response = get_openai_response(
                    system_prompt=load_prompt(task='generate_high_level_answer', role='system'),
                    user_prompt=load_prompt(task='generate_high_level_answer', role='user').format(
                        tables_info=tables_info,
                        high_level_question=high_level_question
                        )
                )
            except Exception as e:
                logger.warning(f"[Error #{idx + 1}-{jdx + 1}] {e}")
                continue
            
            generate_each_high_level_answer_result += f"[Annotated answer]\n{response}\n"
            llm_response_buffer['system_prompt'].append(system_prompt)
            llm_response_buffer['user_prompt'].append(user_prompt)
            llm_response_buffer['response'].append(response)

            tqdm.write(f"[Done #{idx + 1}-{jdx + 1}] Generate high-level answer using modifed question and gold table set information.")

            annotate_questions_and_answers_result += "\n==\n\n"
            annotate_questions_and_answers_result += f"> About generated question #{jdx + 1} . . .\n[Generated question]\n{generated_question}\n"
            annotate_questions_and_answers_result += verify_and_modify_generated_each_question_result
            annotate_questions_and_answers_result += generate_each_high_level_answer_result

        with open(f'annotation_result_{idx + 1}.txt', 'w') as file:
            file.write(display_info)
            file.write("==\n\n")
            file.write(generate_high_level_questions_result)
            file.write(annotate_questions_and_answers_result)


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = parser()
    args, _ = parser.parse_known_args()
    logger.info(args)

    main(
        dataset_name = args.d,
        sample_n = args.n
    )
