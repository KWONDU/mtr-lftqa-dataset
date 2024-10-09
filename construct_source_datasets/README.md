# Construction process

## Table2text

    table2text_match_wikipedia_page_title.py
            -> extract_wikipedia_category_set_using_beautifulsoap.py
            -> extract_wikipedia_subject_set.py
            -> cluster_page.py
            -> ...
            -> construct_source_dataset.py

## Text2SQL

    text2sql_modify_multitabqa_dataset.py
          -> generate_statements.py
          -> modify_wrong_statements.py
          -> construct_source_dataset.py
