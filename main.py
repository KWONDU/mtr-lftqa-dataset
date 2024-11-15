import argparse
import json
import logging
import random
from collections import defaultdict
from typing import Any, Dict, List, Literal, Union


def main(
        table_lake: Dict[str, Dict[str, Any]],
        instance_set: List[Dict[str, Any]],
        classification: Literal['high_header_sim', 'low_header_sim'],
        model_name: str,
        batch_size: int,
        flag: List[Union[bool, None]],
        logger: logging.Logger
    ):
    """dataset annotation framework

    [Params]
    table_lake     : Dict[str, Dict[str, Any]]
    instance_set   : List[Dict[str, Any]]
    classification : Literal['high_header_sim', 'low_header_sim']
    model_name     : str
    batch_size     : int
    flag           : List[Union[bool, None]]
    logger         : logging.Logger
    """
    ### STEP 1 ### : Gold table set construction
    if flag[1] is not None:
        exit() # gold table set is already constructed
    ### STEP 1 ###

    ### STEP 2 ### : Question annotation
    if flag[2]:
        from steps.annotate_questions import annotate_questions
        if classification == 'high_header_sim':
            from get_shots import get_annotate_questions_with_high_header_sim_task_shots as get_annotate_questions_task_shots
        elif classification == 'low_header_sim':
            from get_shots import get_annotate_questions_with_low_header_sim_task_shots as get_annotate_questions_task_shots
    
        logger.info("> Step 2")
        logger.info(f"[{'Start':<7}]: annotate questions.")
        high_level_question_set, success_cnt, fail_cnt, cost = annotate_questions(
            table_lake=table_lake,
            instance_set=instance_set,
            classification=classification,
            load_shot=get_annotate_questions_task_shots,
            model_name=model_name,
            batch_size=batch_size
        )

        with open(f'results/storage/{classification}/high_level_question_set.json', 'w') as file:
            json.dump(high_level_question_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate questions.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 2 ###

    ### STEP 3 ### : Table document construction
    if flag[3]:
        from steps.expand_statement import expand_statement
        if classification == 'high_header_sim':
            from get_shots import get_expand_statement_with_high_header_sim_task_shots as get_expand_statement_task_shots
        elif classification == 'low_header_sim':
            from get_shots import get_expand_statement_with_low_header_sim_task_shots as get_expand_statement_task_shots

        logger.info("> Step 3")
        logger.info(f"[{'Start':<7}]: expand statement.")
        table_document_set, success_cnt, fail_cnt, cost = expand_statement(
            table_lake=table_lake,
            instance_set=instance_set,
            classification=classification,
            load_shot=get_expand_statement_task_shots,
            model_name=model_name,
            batch_size=batch_size
        )

        with open(f'results/storage/{classification}/table_document_set.json', 'w') as file:
            json.dump(table_document_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: expand statement.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 3 ###

    ### STEP 4 ### : Relevant data extraction
    if flag[4]:
        from steps.extract_relevant_data import extract_relevant_data
        with open(f'results/storage/{classification}/table_document_set.json', 'r') as file:
            table_document_set = json.load(file)
        with open(f'results/storage/{classification}/high_level_question_set.json', 'r') as file:
            high_level_question_set = json.load(file)
        
        logger.info("> Step 4")
        logger.info(f"[{'Start':<7}]: extract relevant data.")
        relevant_data_set, success_cnt, fail_cnt, cost = extract_relevant_data(
            table_lake=table_lake,
            table_document_set=table_document_set,
            high_level_question_set=high_level_question_set,
            classification=classification,
            model_name=model_name,
            batch_size=batch_size
        )

        with open(f'results/storage/{classification}/relevant_data_set.json', 'w') as file:
            json.dump(relevant_data_set, file, indent=4)
    
        logger.info(f"[{'Done':<7}]: extract relevant data.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 4 ###

    ### STEP 5 ### : Answer annotation
    if flag[5]:
        from steps.annotate_answer import annotate_answer
        with open(f'results/storage/{classification}/relevant_data_set.json', 'r') as file:
            relevant_data_set = json.load(file)

        logger.info("> Step 5")
        logger.info(f"[{'Start':<7}]: annotate answer.")
        high_level_qa_pair_set, success_cnt, fail_cnt, cost = annotate_answer(
            table_lake=table_lake,
            relevant_data_set=relevant_data_set,
            classification=classification,
            model_name=model_name,
            batch_size=batch_size
        )

        with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'w') as file:
            json.dump(high_level_qa_pair_set, file, indent=4)
        
        logger.info(f"[{'Done':<7}]: annotate answer.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}.")
    ### STEP 5 ###

    ### STEP 6 ### : Validation
    if flag[6]:
        from steps.validate import validate
        with open(f'results/storage/{classification}/high_level_qa_pair_set.json', 'r') as file:
            high_level_qa_pair_set = json.load(file)
    
        logger.info("> Step 6")
        logger.info(f"[{'Start':<7}]: validate.")
        high_level_qa_pair_set_with_validation, success_cnt, fail_cnt, cost = validate(
            table_lake=table_lake,
            high_level_qa_pair_set=high_level_qa_pair_set,
            classification=classification,
            model_name=model_name,
            batch_size=batch_size
        )

        with open(f'results/storage/{classification}/high_level_qa_pair_set_with_validation.json', 'w') as file:
            json.dump(high_level_qa_pair_set_with_validation, file, indent=4)

        logger.info(f"[{'Done':<7}]: validate.")
        logger.info(f"[{'Cost':<7}]: ${cost:.2f}.")
        logger.info(f"[{'Results':<7}]: success {success_cnt}, fail {fail_cnt}")
    ### STEP 6 ###

    ### STEP 7 ### : Filtering
    if flag[7]:
        with open(f'results/storage/{classification}/high_level_qa_pair_set_with_validation.json', 'r') as file:
            high_level_qa_pair_set_with_validation = json.load(file)
        
        logger.info("> Step 7")
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

        # Display
        true_cnt = defaultdict(int)
        false_cnt = defaultdict(int)
        null_cnt = defaultdict(int)
        true_chk = []

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

        logger.info("[True ]\t" + "\t".join([f"{item[0]}: {item[1]}" for item in sorted(list(true_cnt.items()))]))
        logger.info("[False]\t" + "\t".join([f"{item[0]}: {item[1]}" for item in sorted(list(false_cnt.items()))]))
        logger.info(f"Zero true: {sum(True for item in true_chk if item == 0)}")
        logger.info(f"One true: {sum(True for item in true_chk if item == 1)}")
        logger.info(f"Two trues: {sum(True for item in true_chk if item == 2)}")
        logger.info(f"Three trues: {sum(True for item in true_chk if item == 3)}")
    ### STEP 7 ###


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

    from steps.regularize import regularize_source_dataset
    from utils.dataset import load_source_dataset
    source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=args.d))

    random.seed(4242)
    table_lake = {tb['id']: tb for tb in source_dataset.tables}
    instance_set = random.sample(source_dataset[:], args.n)
    MODEL_NAME = 'gpt-4o-mini'
    BATCH_SIZE = 50

    CLASS = {
        'SourceOpenWikiTable': 'high_header_sim',
        'SourceSpiderTableQA': 'low_header_sim'
    }
    
    FLAG = [None, None, False, False, False, False, False, False]

    main(
        table_lake=table_lake,
        instance_set=instance_set,
        classification=CLASS[args.d],
        model_name=MODEL_NAME,
        batch_size=BATCH_SIZE,
        flag=FLAG,
        logger=logger
    )
