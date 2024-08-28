#!/bin/bash

required_files=("dataset.py" "main.py" "util.py" "source_data/spider/tables.json")
for files in "${required_files[@]}"
do
    do
    if [[ ! -f $file ]]; then
        echo "ERROR: '$file' is missing."
        exit 1
    fi
done

pip install datasets nltk numpy torch openai tqdm transformers

echo "INFO: setup done."
