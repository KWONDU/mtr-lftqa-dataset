# Multi-table retrieval + Long-form table QA dataset

## File construction

    [folder] dataset
        [folder] dump
            [file] {dataset_name}*.pkl  # source dataset checkpoints
        [file] dataset_stats.py
        [file] load_{dataset_name}.py # dataset loader (no need to use directly)
    [folder] prompt
        [file] system_prompt_template.txt
        [file] user_prompt_template.txt
    [folder] results
        [file] dataset_statistics.csv
    [file] main.py
    [file] requirements.txt # setup file
    [file] util_dill.py
    [file] util.py

## .gitignore

    .gitignore
    *__pycache__
    dataset/source
    dataset/huggingface_access_token.yaml

## 1. Setup

    pip install -r requirements.txt

## 2. Load dataset

    from util import load_dataset

    dataset = load_dataset({dataset_name})
