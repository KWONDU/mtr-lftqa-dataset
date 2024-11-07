import argparse
import logging
import json
import random
from collections import defaultdict
from utils.dataset import load_source_dataset


def display_stats(high_level_qa_pair_set_with_validation: object) -> None:
    """display stats
    
    [Param]
    high_level_qa_pair_set_with_validation : object
    """
    true_cnt = defaultdict(int)
    false_cnt = defaultdict(int)
    null_cnt = defaultdict(int)
    true_chk = list()

    for dataset in high_level_qa_pair_set_with_validation:
        for data in dataset['annotation']:
            chk = 0
            for key, value in data['validation'].items():
                if value == True:
                    true_cnt[key] += 1
                    chk += 1
                elif value == False:
                    false_cnt[key] += 1
                else:
                    null_cnt[key] += 1
            true_chk.append(chk)

    print(dict(sorted(list(true_cnt.items()))))
    print(dict(sorted(list(false_cnt.items()))))
    print(f"Zero true: {sum(True for _ in true_chk if _ == 0)}")
    print(f"One true: {sum(True for _ in true_chk if _ == 1)}")
    print(f"Two trues: {sum(True for _ in true_chk if _ == 2)}")
    print(f"Three trues: {sum(True for _ in true_chk if _ == 3)}")
    return None


def main(source_dataset_name, sample_n, classification, logger):
    from steps.regularize import regularize_source_dataset
    source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=source_dataset_name))

    random.seed(4242)
    table_lake = {tb['id']: tb for tb in source_dataset.tables}
    instance_set = random.sample(source_dataset[:], sample_n)
    MODEL_NAME = 'gpt-4o-mini'
    SEMAPHORE_VALUE = 50

    ### STEP 1 ### : Table document construction
    if FLAG[1]:
        from steps.expand_statement import expand_statement
        if classification == 'high_header_sim':
            from get_shots import get_expand_statement_with_high_header_sim_task_shots as get_expand_statement_task_shots
        elif classification == 'low_header_sim':
            from get_shots import get_expand_statement_with_low_header_sim_task_shots as get_expand_statement_task_shots

        logger.info("> Step 1")
        logger.info(f"[{'Start':<7}]: expand statement.")
        table_document_set, success_cnt, fail_cnt, cost = expand_statement(
            table_lake=table_lake,
            instance_set=instance_set,
            classification=classification,
            load_shot=get_expand_statement_task_shots,
            model_name=MODEL_NAME,
            semaphore_value=SEMAPHORE_VALUE
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
        if classification == 'high_header_sim':
            from get_shots import get_annotate_questions_with_high_header_sim_task_shots as annotate_questions_task_shots
        elif classification == 'low_header_sim':
            from get_shots import get_annotate_questions_with_low_header_sim_task_shots as annotate_questions_task_shots
    
        logger.info("> Step 3")
        logger.info(f"[{'Start':<7}]: annotate questions.")
        high_level_question_set, success_cnt, fail_cnt, cost = annotate_questions(
            table_lake=table_lake,
            instance_set=instance_set,
            classification=classification,
            load_shot=annotate_questions_task_shots,
            model_name=MODEL_NAME,
            semaphore_value=SEMAPHORE_VALUE
        )

        with open(f'results/storage/{classification}/high_level_question_set.json', 'w') as file:
            json.dump(high_level_question_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate questions.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 3 ###

    ### STEP 4 ### : Relevant data extraction
    if FLAG[4]:
        from steps.extract_relevant_data import extract_relevant_data
        with open(f'results/storage/high_header_sim/table_document_set.json', 'r') as file:
            table_document_set = json.load(file)
        with open(f'results/storage/high_header_sim/high_level_question_set.json', 'r') as file:
            high_level_question_set = json.load(file)
            
        logger.info("> Step 4")
        logger.info(f"[{'Start':<7}]: extract relevant data.")
        relevant_data_set, success_cnt, fail_cnt, cost = extract_relevant_data(
            table_lake=table_lake,
            table_document_set=table_document_set,
            high_level_question_set=high_level_question_set,
            classification=classification,
            model_name=MODEL_NAME,
            semaphore_value=SEMAPHORE_VALUE
        )

        with open(f'results/storage/high_header_sim/relevant_data_set.json', 'w') as file:
            json.dump(relevant_data_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: extract relevant data.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 4 ###

    ### STEP 5 ### : Answer annotation
    if FLAG[5]:
        from steps.annotate_answer import annotate_answer
        with open(f'results/storage/{classification}/relevant_data_set.json', 'r') as file:
            relevant_data_set = json.load(file)

        logger.info("> Step 5")
        logger.info(f"[{'Start':<7}]: annotate answer.")
        high_level_qa_pair_set, success_cnt, fail_cnt, cost = annotate_answer(
            table_lake=table_lake,
            relevant_data_set=relevant_data_set,
            classification=classification,
            model_name=MODEL_NAME,
            semaphore_value=SEMAPHORE_VALUE
        )

        with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'w') as file:
            json.dump(high_level_qa_pair_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate answer.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 5 ###

    ### STEP 6 ### : Validation
    if FLAG[6]:
        from steps.validate import validate
        with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'r') as file:
            high_level_qa_pair_set = json.load(file)
    
        logger.info("> Step 6")
        logger.info(f"[{'Start':<7}]: validate.")
        high_level_qa_pair_set_with_validation, success_cnt, fail_cnt, cost = validate(
            table_lake=table_lake,
            high_level_qa_pair_set=high_level_qa_pair_set,
            classification=classification,
            model_name=MODEL_NAME,
            semaphore_value=SEMAPHORE_VALUE
        )

        with open(f'results/storage/{classification}/high_level_qa_pair_set_with_validation.json', 'w') as file:
            json.dump(high_level_qa_pair_set_with_validation, file, indent=4)

        logger.info(f"[{'Done':<7}]: validate.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}")
    ### STEP 6 ###

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
                        'gold_table_id_set': instance['gold_table_id_set'],
                        'question': annotation['question'].strip().replace('\n', ' '),
                        'answer': annotation['answer'].strip().replace('\n', ' ')
                    }
                )
            else:
                error_cases.append(
                    {
                        'gold_table_set': [table_lake[table_id] for table_id in instance['gold_table_id_set']],
                        'question': annotation['question'].strip().replace('\n', ' '),
                        'answer': annotation['answer'].strip().replace('\n', ' '),
                        'validation': annotation['validation']
                    }
                )
    
    with open(f'results/{classification}_dataset.json', 'w') as file:
        json.dump(dataset, file, indent=4)
    
    with open(f'buffer/{classification}/error_cases.json', 'w') as file:
        json.dump(error_cases, file, indent=4)
    
    logger.info(f"[{'Done':<7}]: filtering.")

    display_stats(high_level_qa_pair_set_with_validation=high_level_qa_pair_set_with_validation)
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
    api_key = '___YOUR_OPENAI_API_KEY___'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """
    
    FLAG = [None, False, None, False, False, False, False]
    FILTER_FLAG = False

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
