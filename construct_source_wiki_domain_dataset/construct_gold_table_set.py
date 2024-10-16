import json
import os
import re
import sys
from collections import defaultdict


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    return load_dataset


if __name__ == '__main__':
    load_dataset = import_utils()

    openwikitable = load_dataset(dataset_name='Open-WikiTable')
    gt_set = defaultdict(set)

    # type I. Same page
    tables_with_same_page = defaultdict(list)
    for table in openwikitable.tables:
        page_title = table['metadata'].split('|')[0].strip()
        tables_with_same_page[page_title].append((table['id'], table['metadata']))
    
    for _, tables in tables_with_same_page.items():
        if len(tables) == 1:
            continue

        gt_set[1].add(tuple(sorted(tables, key=lambda x: x[0]))) # unique gold table set

    # type II. Similar pages
    tables_with_similar_page = defaultdict(list)
    for table in openwikitable.tables:
        page_title_with_no_num = re.sub(r'\d+', '', table['metadata'].split('|')[0]).strip()
        tables_with_similar_page[page_title_with_no_num].append((table['id'], table['metadata']))
    
    for _, tables in tables_with_similar_page.items():
        if len(tables) == 1:
            continue

        if tuple(sorted(tables, key=lambda x: x[0])) not in gt_set[1]:
            gt_set[2].add(tuple(sorted(tables, key=lambda x: x[0]))) # remove duplicate gold table set

    # type III. Different pages
    None

    # STATS
    gt_list = [
        [
            {
                'id': tb[0],
                'metadata': tb[1],
                'type': type
            }
            for tb in gt
        ]
        for type, gt_set in gt_set.items()
        for gt in gt_set
    ]

    with open('storage/gold_table_set.json', 'w') as file:
        json.dump(gt_list, file, indent=4)
