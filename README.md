# Multi-table retrieval + Long-form table QA dataset

## File construction

    [folder] dataset
        [folder] dump # source dataset checkpoints, *.pkl
        [folder] source # source dataset files, only local (too big size)
        [file] dataset_stats.csv # dataset statistics
        [file] dataset_stats.py
        [file] huggingface_access_token.yaml # only local
        [file] load_{dataset_name}.py # dataset loader (no need to use directly)
    [folder] prompt
        [file] system_prompt_template.txt
        [file] user_prompt_template.txt
    [file] .gitignore # not upload
    [file] main.py
    [file] requirements.txt # setup file
    [file] util_dill.py
    [file] util.py

## 1. Setup

    pip install -r requirements.txt

## 2. Load dataset

    from util import load_dataset

    dataset = load_dataset({dataset_name})
