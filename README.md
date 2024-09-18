# Multi-table retrieval + Long-form table QA dataset

## File construction

    [folder] dataset
        [folder] dump
            [file] {dataset_name}*.pkl  # source dataset checkpoints
        [file] dataset_stats.py
        [file] load_{dataset_name}.py # dataset loader (no need to use directly)
    [folder] prompt
        [folder] system
            [file] generate_high_level_answer.txt # step 3 system promt
            [file] generate_high_level_questions.txt # step 1 system prompt
            [file] verify_and_modify_generated_question.txt # step 2 system prompt
        [folder] user
            [file] generate_high_level_answer.txt # step 3 user prompt
            [file] generate_high_level_questions.txt # step 1 user prompt
            [file] verify_and_modify_generated_question.txt # step 2 user prompt
    [folder] results
        [file] annotation_result_{ith}.txt # QA pair annotation result
        [file] dataset_statistics.csv
        [file] llm_responses.txt # full prompts and responses
    [file] main.py
    [file] requirements.txt # setup file
    [file] util_dill.py
    [file] util_format.py
    [file] util_llm.py
    [file] util.py

## .gitignore

    .gitignore
    *__pycache__
    dataset/source
    dataset/huggingface_access_token.yaml

## 1. Setup

    pip install -r requirements.txt

## 2. About dataset

### 2.1 Load dataset

    from util import load_dataset

    dataset = load_dataset({dataset_name})

### 2.2 Dataset configuration

    dataset
        download_type # [str] huggingface or local
        tables
            [
                {
                    'id': [str] each table's ID
                    'metadata': [str] each table's metadata
                    'metadata_info': [str] metadata configuration process
                    'header': [list] each table's header
                    'cell': [2d list] each table's cells
                    'source': [str] each table's source (None in case not specified)
                },
                . . .
            ]
        dataset.train # about train set
            [
                {
                    'gold_tables': [list] gold table IDs
                    'question': [str] each data's annotated question
                    'answer': [str] each data's annotated answer # can be tuple
                    'answer_type': [str] sentence or table or word or SQL or T/F # can be tuple
                },
                . . .
            ]
        dataset.validation # about validation set
            # DITTO
        dataset.test    # about test set
            # DITTO

    print(dataset) # return dataset name
    print(dataset[i]) # return i'th data; train, validation, test set in order
    print(len(dataset)) # return total dataset size
