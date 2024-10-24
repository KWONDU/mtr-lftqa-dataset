import argparse
import asyncio
import logging
import json
import random
from utils.dataset import load_source_dataset


def main(source_dataset_name, sample_n, type, logger):
    if not FLAG[0]:
        exit()

    source_dataset = load_source_dataset(dataset_name=source_dataset_name)

    table_lake = {tb['id']: tb for tb in source_dataset.tables}
    random.seed(42)
    instance_set = random.sample(source_dataset[:], sample_n)

    semaphore = asyncio.Semaphore(100)
    MODEL_NAME = 'gpt-3.5-turbo'

    ### STEP 1 ###
    if FLAG[1]:
        if type == 'low_header_sim':
            from steps.low_header_sim.annotate_questions import annotate_questions
            from get_shots import get_annotate_questions_task_shots_with_low_header_sim as get_annotate_questions_task_shots
        elif type == 'high_header_sim':
            from steps.high_header_sim.annotate_questions import annotate_questions
            from get_shots import get_annotate_questions_task_shots_with_high_header_sim as get_annotate_questions_task_shots

        logger.info("> Step 1")
        logger.info(f"[{'Start':<7}]: annotate questions.")
        high_level_question_set, success_cnt, fail_cnt, cost = annotate_questions(
            table_lake=table_lake,
            instance_set=instance_set,
            load_shot=get_annotate_questions_task_shots,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{type}/high_level_question_set.json', 'w') as file:
            json.dump(high_level_question_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate questions.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 1 ###

    ### STEP 2 ###
    if FLAG[2]:
        if type == 'low_header_sim':
            from steps.low_header_sim.expand_statement import expand_statement
            from get_shots import get_expand_statement_task_shots_with_low_header_sim as get_expand_statement_task_shots
        elif type == 'high_header_sim':
            from steps.high_header_sim.expand_statement import expand_statement
            from get_shots import get_expand_statement_task_shots_with_high_header_sim as get_expand_statement_task_shots

        logger.info("> Step 2")
        logger.info(f"[{'Start':<7}]: expand statement.")
        table_document_set, success_cnt, fail_cnt, cost = expand_statement(
            table_lake=table_lake,
            instance_set=instance_set,
            load_shot=get_expand_statement_task_shots,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{type}/table_document_set.json', 'w') as file:
            json.dump(table_document_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: expand statement.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 2 ###

    ### STEP 3 ###
    if FLAG[3]:
        if type == 'low_header_sim':
            from steps.low_header_sim.annotate_answer import annotate_answer
        elif type == 'high_header_sim':
            from steps.high_header_sim.annotate_answer import annotate_answer

        logger.info("> Step 3")
        logger.info(f"[{'Start':<7}]: annotate answer.")
        high_level_qa_pair_set, success_cnt, fail_cnt, cost = annotate_answer(
            table_lake=table_lake,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{type}/high_level_qa_pair_set.json', 'w') as file:
            json.dump(high_level_qa_pair_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate answer.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 3 ###

    ### STEP 4 ###
    if FLAG[4]:
        from steps.validate import validate
    
        logger.info("> Step 4")
        logger.info(f"[{'Start':<7}]: validate.")
        high_level_qa_pair_set_with_validation, success_cnt, fail_cnt, cost = validate(
            type=type,
            table_lake=table_lake,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{type}/high_level_qa_pair_set_with_validation.json', 'w') as file:
            json.dump(high_level_qa_pair_set_with_validation, file, indent=4)

        logger.info(f"[{'Done':<7}]: validate.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}")
    ### STEP 4 ###

    ### STEP 5 ###
    if FLAG[5]:
        logger.info("> Step 5")
        logger.info(f"[{'Start':<7}]: filtering.")

        with open(f'results/storage/{type}/high_level_qa_pair_set_with_validation.json', 'r') as file:
            high_level_qa_pair_set_with_validation = json.load(file)

        dataset = [
            {
                'gold_table_id_set': sorted(instance['gold_table_id_set']),
                'question': annotation['question'].strip().replace('\n', ' '),
                'answer': annotation['answer'].strip().replace('\n', ' ')
            }
            for instance in high_level_qa_pair_set_with_validation
            for annotation in instance['annotation']
            if (
                annotation['validation']['gold_table_set']
                and
                annotation['validation']['annotated_question']
                and
                annotation['validation']['annotated_answer']
            )
        ]

        with open(f'results/{type}_dataset.json', 'w') as file:
            json.dump(dataset, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: filtering.")
    ### STEP 5 ###


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, required=True, choices=['SourceMultiTabQA', 'SourceOpenWikiTable'], help='dataset name')
    parser.add_argument('-n', type=int, required=True, help='number of sampled data')

    args, _ = parser.parse_known_args()
    logger.info(args)

    """
    api_key = '___YOUR_OWN_API_KEY___'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """

    FLAG = [True, False, False, False, True, True]

    TYPE = {
        'SourceMultiTabQA': 'low_header_sim',
        'SourceOpenWikiTable': 'high_header_sim'
    }

    main(
        source_dataset_name=args.d,
        sample_n=args.n,
        type=TYPE[args.d],
        logger=logger
    )
