# Multi-table retrieval + Long-form table QA dataset

## Package 'utils'

    utils
        .dataset
            .AbstractDataset
            .add_huggingface_access_token(token) -> token
            .load_dataset(dataset_name) -> dataset
            .save_dataset(dataset, dataset_name) -> [pkl_file_list]
        .format
            .data_format(data_num, question, sql, sub_table) -> data_visualization
            .table_format(table_num, metadata, header, cell) -> table_visualization
        .openai
            .add_openai_api_key(api_key) -> api_key
            .get_openai_response(system_prompt, user_prompt, llm='gpt-3.5') -> system_prompt, user_prompt, response
            .load_prompt(role, task) -> prompt
            .remove_prompt(role, task) -> bool
            .save_prompt(file_path, role, task) -> {role: task}
            .view_prompt_list() -> {role: [task_list], ...}

## File construction

    [folder] plans
        [file] step0.py
        [file] step1.py
        [file] step2.py
        [file] step3.py
    [folder] prompt
        [folder] system
            [file] generate_high_level_answer.txt
            [file] generate_high_level_questions.txt
            [file] verify_and_modify_generated_question.txt
        [folder] user
            [file] generate_high_level_answer.txt
            [file] generate_high_level_questions.txt
            [file] verify_and_modify_generated_question.txt
    [folder] results
        [file] annotation_result_{ith}.txt
        [file] dataset_statistics.csv
        [file] llm_responses.txt
    [package] utils
    [file] dataset_stats.py
    [file] main.py
    [file] README.md
    [file] requirements.txt

## .gitignore

### .gitignore

    .gitignore
    *__pycache__
    *.yaml

### utils/.gitignore

    dataset/_class/source
    openai/_prompt

## 1. Setup

    pip install -r requirements.txt

### 1.1 Setup token/key

    from utils.dataset import add_huggingface_access_token
    from utils.openai import add_openai_api_key

    add_huggingface_access_token(token=token)
    add_openai_api_key(api_key=api_key)

### 1.2 Setup prompt

    from utils.openai import save_prompt

    for role in ['system', 'user']:
        for task in ['generate_high_level_questions', 'verify_and_modify_generated_question', 'generate_high_level_answer']:
            save_prompt(f'prompt/{role}/{task}.txt', role, task)

## 2. About dataset

### 2.1 Load dataset

    from utils.dataset import load_dataset

    dataset = load_dataset(dataset_name=dataset_name)

### 2.2 Dataset configuration

    dataset
        .download_type # [str] huggingface or local
        .tables
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
        .train # about train set
            [
                {
                    'gold_tables': [list] gold table IDs
                    'question': [str] each data's annotated question
                    'answer': [str] each data's annotated answer # can be tuple
                    'answer_type': [str] sentence or table or word or SQL or T/F # can be tuple
                },
                . . .
            ]
        .validation # about validation set
            # DITTO
        .test    # about test set
            # DITTO

    print(dataset) # return dataset name
    print(dataset[i]) # return i'th data; train, validation, test set in order
    print(len(dataset)) # return total dataset size

## 3. python3 main.py

    python3 main.py \
        -d {dataset_name, default: MultiTabQA}
        -n {number_of_sampled_data, default: 1}
        -k {whether add openai api key, default: True}
