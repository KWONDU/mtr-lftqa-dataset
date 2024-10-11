import json
import os
import sys
from collections import defaultdict


def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:  # Unix (e.g. Linux, macOS)
        os.system('clear')


def get_page_title():
    user_input = input("\nModified page title (just press ENTER if searched page title is correct): ")
    return user_input


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    from utils.display import table_visualization
    return load_dataset, table_visualization

if __name__ == '__main__':
    load_dataset, table_visualization = import_utils()

    ###
    openwikitable = load_dataset(dataset_name='Open-WikiTable')

    match_page_title_table_info = defaultdict(list)
    for table in openwikitable.tables:
        page_title, _, _ = tuple(table['metadata'].split(' | '))
        match_page_title_table_info[page_title].append(table)
    ###

    with open('storage/matched_wikipedia_page_title.json', 'r') as file:
        matched_page_title = json.load(file)
    
    diff_page_title_dict = {
        org_page_title: gold_page_title
        for org_page_title, gold_page_title in matched_page_title.items()
        if org_page_title != gold_page_title
    }
    
    modified_page_title_set_with_human_annotation = dict()
    for idx, (org_page_title, searched_page_title) in enumerate(diff_page_title_dict.items()):
        clear_screen()

        print(f"{idx + 1} / {len(diff_page_title_dict)}")

        print(f"Orginal page title: {org_page_title}")
        print(f"Searched page title: {searched_page_title}")

        print("All table's info:\n")
        for jdx, table in enumerate(match_page_title_table_info[org_page_title]):
            print(table_visualization(table_num=jdx+1, metadata=table['metadata'], header=table['header'], cell=table['cell']))
            print()

        modified_page_title = get_page_title()
        if modified_page_title == "":
            continue

        modified_page_title_set_with_human_annotation[org_page_title] = modified_page_title
    
    with open('storage/modified_page_title_set_with_human_annotation.json', 'w') as file:
        json.dump(modified_page_title_set_with_human_annotation, file, indent=4)
