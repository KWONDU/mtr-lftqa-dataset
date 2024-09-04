#!/bin/bash

required_files=("dataset.py" "main.py" "util_llm.py" "util_manip.py" "util.py")
for files in "${required_files[@]}"
do
    do
    if [[ ! -f $file ]]; then
        echo "ERROR: '$file' is missing."
        exit 1
    fi
done

pip install nltk numpy torch openai tqdm transformers
tar -zxvf source_data/tabfact/tables.tar.gz

echo "INFO: setup done."
