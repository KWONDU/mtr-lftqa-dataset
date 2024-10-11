# README

![Main](results/main.png)

## Package 'utils'

    utils
        .dataset
            .AbstractDataset
            .add_huggingface_access_token(token) -> token
            .load_dataset(dataset_name) -> dataset
            .load_source_dataset(dataset_name) -> source_dataset
            .save_dataset(dataset, dataset_name) -> [pkl_file_list]
            .save_source_dataset(dataset_name) -> [pkl_file_list]
        .display
            .table_serialization(table_num, metadata, header, cell) -> table_serialization
            .table_visualization(table_num, metadata, header, cell) -> table_visualization
        .openai
            .add_openai_api_key(api_key) -> api_key
            .get_async_openai_response(semaphore, system_prompt, user_prompt, model_name, key)
                -> Dict(
                    'system_prompt',
                    'user_prompt',
                    'response',
                    'input_tokens_cost',
                    'output_tokens_cost',
                    'key'
                )
            .load_llm(model_name) -> llm
            .load_openai_api_key() -> openai_api_key
            .load_prompt(role, task) -> prompt
            .remove_prompt(role, task) -> bool
            .save_prompt(file_path, role, task) -> {role: task}
            .view_prompt_list() -> {role: [task_list], ...}

## .gitignore

    .gitignore
    *__pycache__
    *.yaml
    *temp*
    *buffer/*
    !*/.gitkeep

### utils/.gitignore

    dataset/_class/source
    openai/_prompt
    !*/.gitkeep

## 1. Setup

    pip install -r requirements.txt

### 1.1 Setup token/key

    from utils.dataset import add_huggingface_access_token
    from utils.openai import add_openai_api_key

    add_huggingface_access_token(token=token)
    add_openai_api_key(api_key=api_key)

### 1.2 Setup prompt

    from utils.openai import save_prompt

    for role in [. . .]: # assistant, system, user
        for task in [. . .]:
            save_prompt(file_path=f'prompt/{role}/{task}.txt', role, task)

## 2. About dataset

### 2.1 Load source dataset

    from utils.dataset import load_source_dataset

    dataset = load_source_dataset(dataset_name=source_dataset_name)

### 2.2 Source dataset configuration

    source_dataset
        .tables
            [
                {
                    'id': [str] each table's ID
                    'metadata': [str] each table's metadata
                    'metadata_info': [str] metadata configuration process
                    'header': [list] each table's header
                    'cell': [2d list] each table's cells
                }
                . . .
            ]
        .train # about train set
            [
                {
                    'gold_table_id_set': [list] gold table IDs
                    'data_list': [
                        {
                            'entailed_table_id_set': [list] entailed table IDs,
                            'statement': [str] statement
                        },
                        . . .
                    ]
                }
                . . .
            ]
        .validation # about validation set
            # DITTO
        .test    # about test set
            # DITTO
    
    print(source_dataset) # return source dataset name
    print(source_dataset[i / i:j]) # return i'th / (i ~ j-1)'th data; train, validation, test set in order
    print(len(source_dataset)) # return total source dataset size

### 2.3 Original dataset configuration (Don't need to use)

![Stats](results/dataset_statistics.png)

    original_dataset
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

    print(original_dataset) # return original dataset name
    print(original_dataset[i / i:j]) # return i'th / (i ~ j-1)'th data; train, validation, test set in order
    print(len(original_dataset)) # return total original dataset size

## 3. Run 'main'

    python3 main.py \
        -d {source_dataset_name, SourceDB or SourceWikipeida}
        -n {number_of_sampled_data}
