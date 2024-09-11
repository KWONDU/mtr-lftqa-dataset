# Multi-table retrieval + Long-form table QA dataset

## File construction

    [folder] dataset
        [folder] dump # source dataset checkpoints
        [folder] source # source dataset files, not upload (too big size)
        [file] dataset_stats.csv # dataset statistics
        [file] dataset_stats.py
        [file] dataset_template.py
        [file] huggingface_access_token.yaml # not upload
        [file] load_{dataset_name}.py # various dataset
    [folder] prompt
        [file] system_prompt_template.txt
        [file] user_prompt_template.txt
    [file] .gitignore # not upload
    [file] main.py
    [file] requirements.txt # setup file
    [file] util.py

## 1. Setup

    pip install -r requirements.txt

## 2. Run

    python3 main.py -d {dataset_name}
