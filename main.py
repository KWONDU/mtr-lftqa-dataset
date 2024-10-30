import argparse
import asyncio
import logging
import json
import random
from utils.dataset import load_source_dataset


def main(source_dataset_name, sample_n, classification, logger):
    from steps.regularize import regularize_source_dataset
    source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=source_dataset_name))

    random.seed(104)
    table_lake = {tb['id']: tb for tb in source_dataset.tables}
    instance_set = random.sample(source_dataset[:], sample_n)
    semaphore = asyncio.Semaphore(100)
    MODEL_NAME = 'gpt-3.5-turbo'

    ### STEP 1 ### : Table document construction
    if FLAG[1]:
        from steps.expand_statement import expand_statement
        if classification == 'high_header_sim':
            from get_shots import get_expand_statement_with_high_header_sim_task_shots as get_expand_statement_task_shots
        elif classification == 'low_header_sim':
            from get_shots import get_expand_statement_with_low_header_sim_task_shots as get_expand_statement_task_shots
        else:
            exit()

        logger.info("> Step 1")
        logger.info(f"[{'Start':<7}]: expand statement.")
        table_document_set, success_cnt, fail_cnt, cost = expand_statement(
            table_lake=table_lake,
            instance_set=instance_set,
            classification=classification,
            load_shot=get_expand_statement_task_shots,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{classification}/table_document_set.json', 'w') as file:
            json.dump(table_document_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: expand statement.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 1 ###

    ### STEP 2 ### : Gold table set construction
    if FLAG[2] is not None:
        exit() # gold table set is already constructed
    ### STEP 2 ###

    ### STEP 3 ### : Question annotation
    if FLAG[3]:
        from steps.annotate_questions import annotate_questions
        with open(f'results/storage/{classification}/table_document_set.json', 'r') as file:
            table_document_set = json.load(file)
    
        logger.info("> Step 3")
        logger.info(f"[{'Start':<7}]: annotate questions.")
        high_level_question_set, success_cnt, fail_cnt, cost = annotate_questions(
            table_lake=table_lake,
            instance_set=instance_set,
            table_document_set=table_document_set,
            classification=classification,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{classification}/high_level_question_set.json', 'w') as file:
            json.dump(high_level_question_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate questions.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 3 ###

    ### STEP 4 ### : Answer annotation
    if FLAG[4]:
        from steps.annotate_answer import annotate_answer
        with open(f'results/storage/{classification}/table_document_set.json', 'r') as file:
            table_document_set = json.load(file)

        with open(f'results/storage/{classification}/high_level_question_set.json', 'r') as file:
            high_level_question_set = json.load(file)

        logger.info("> Step 4")
        logger.info(f"[{'Start':<7}]: annotate answer.")
        high_level_qa_pair_set, success_cnt, fail_cnt, cost = annotate_answer(
            table_lake=table_lake,
            table_document_set=table_document_set,
            high_level_question_set=high_level_question_set,
            classification=classification,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'w') as file:
            json.dump(high_level_qa_pair_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate answer.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 4 ###

    ### STEP 5 ### : Validation
    if FLAG[5]:
        from steps.validate import validate
        with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'r') as file:
            high_level_qa_pair_set = json.load(file)
    
        logger.info("> Step 5")
        logger.info(f"[{'Start':<7}]: validate.")
        high_level_qa_pair_set_with_validation, success_cnt, fail_cnt, cost = validate(
            table_lake=table_lake,
            high_level_qa_pair_set=high_level_qa_pair_set,
            classification=classification,
            model_name=MODEL_NAME,
            semaphore=semaphore
        )

        with open(f'results/storage/{classification}/high_level_qa_pair_set_with_validation.json', 'w') as file:
            json.dump(high_level_qa_pair_set_with_validation, file, indent=4)

        logger.info(f"[{'Done':<7}]: validate.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}")
    ### STEP 5 ###

    ### Filtering ###
    if not FILTER_FLAG:
        exit()
    
    with open(f'results/storage/{classification}/high_level_qa_pair_set_with_validation.json', 'r') as file:
        high_level_qa_pair_set_with_validation = json.load(file)
    
    logger.info(f"[{'Start':<7}]: filtering.")
    
    dataset = []
    error_cases = []

    for instance in high_level_qa_pair_set_with_validation:
        for annotation in instance['annotation']:
            if (
                annotation['validation']['table_and_question']
                and
                annotation['validation']['table_and_answer']
                and
                annotation['validation']['question_and_answer']
            ):
                dataset.append(
                    {
                        'gold_table_id_set': sorted(instance['gold_table_id_set']),
                        'question': annotation['question'].strip().replace('\n', ' '),
                        'answer': annotation['answer'].strip().replace('\n', ' ')
                    }
                )
            else:
                error_cases.append(
                    {
                        'gold_table_set': [table_lake[table_id] for table_id in sorted(instance['gold_table_id_set'])],
                        'question': annotation['question'].strip().replace('\n', ' '),
                        'answer': annotation['answer'].strip().replace('\n', ' '),
                        'validation': annotation['validation']
                    }
                )

    with open(f'results/{classification}_dataset.json', 'w') as file:
        json.dump(dataset, file, indent=4)
    
    with open(f'results/{classification}_error_cases.json', 'w') as file:
        json.dump(error_cases, file, indent=4)
    
    logger.info(f"[{'Done':<7}]: filtering.")
    ### Filtering ###


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, required=True, choices=['SourceOpenWikiTable', 'SourceSpiderTableQA'], help='dataset name')
    parser.add_argument('-n', type=int, required=True, help='number of sampled data')

    args, _ = parser.parse_known_args()
    logger.info(args)

    """
    api_key = '___YOUR_OWN_API_KEY___'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """

    FLAG = [None, False, None, True, True, True]
    FILTER_FLAG = True

    CLASS = {
        'SourceOpenWikiTable': 'high_header_sim',
        'SourceSpiderTableQA': 'low_header_sim'
    }

    main(
        source_dataset_name=args.d,
        sample_n=args.n,
        classification=CLASS[args.d],
        logger=logger
    )
