import json
import os
import re
import sys
from collections import defaultdict
from tqdm import tqdm


def are_headers_similar(header1, header2):
    header1 = set([col.lower().strip() for col in header1])
    header2 = set([col.lower().strip() for col in header2])
    diff1 = len(header1 - header2)
    diff2 = len(header2 - header1)
    return max(diff1, diff2) <= 1


def decompose_to_3x_4y_with_min_diff(n):
    best_x, best_y = None, None
    min_diff = float('inf')

    for x in range(n // 3 + 1):
        for y in range(n // 4 + 1):
            if 3 * x + 4 * y == n:
                diff = abs(x - y)
                if diff < min_diff:
                    best_x, best_y = x, y
                    min_diff = diff
    
    return best_x, best_y


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset, save_source_dataset
    return load_dataset, save_source_dataset


if __name__ == '__main__':
    load_dataset, save_source_dataset = import_utils()

    openwikitable = load_dataset(dataset_name='Open-WikiTable')
    
    tables = openwikitable.tables
    table_id_set = [table['id'] for table in tables]
    table_lake = {table['id']: table for table in tables}
    print("Load Open-WikiTable dataset done.")

    # gold_table_id_set: delimiter_list
    gold_table_id_set_with_delimiter = defaultdict(list)

    # All gold table set = similar header tables
    # page_title | section_title | table_title
    # overlap check w/ remove number

    # 1. Cluster tables w/ similar header
    try:
        with open('buffer/table_cluster_with_similar_header.json', 'r') as file:
            table_cluster_with_similar_header = json.load(file)
    except:
        table_cluster_with_similar_header = set()
        for ith_table_id in tqdm(table_id_set, desc="Step 1"):
            ith_table_cluster = set()
            for jth_table_id in table_id_set:
                if are_headers_similar(table_lake[ith_table_id]['header'], table_lake[jth_table_id]['header']):
                    ith_table_cluster.add(jth_table_id) # consider i = j, too
            ith_table_cluster = tuple(sorted(ith_table_cluster)) # unique table cluster
            table_cluster_with_similar_header.add(ith_table_cluster)
        
        table_cluster_with_similar_header = list(table_cluster_with_similar_header)
        with open('buffer/table_cluster_with_similar_header.json', 'w') as file:
            json.dump(table_cluster_with_similar_header, file, indent=4)
    print("Step 1 done.")
    
    # 2. Cluster tables w/ overlap at least one title
    table_cluster_with_similar_header_and_overlap_at_least_one_title = defaultdict(set)
    for cdx, table_cluster in enumerate(table_cluster_with_similar_header): # cdx : cluster with similar header
        for table_id in table_cluster:
            table = table_lake[table_id]
            page_title = re.sub(r'\d+', '', table['metadata'].split('|')[0].lower().strip())
            section_title = re.sub(r'\d+', '', table['metadata'].split('|')[1].lower().strip())
            table_title = re.sub(r'\d+', '', table['metadata'].split('|')[2].lower().strip())
            table_cluster_with_similar_header_and_overlap_at_least_one_title[(cdx, -1, page_title, None, None)].add(table['id'])
            table_cluster_with_similar_header_and_overlap_at_least_one_title[(cdx, -1, None, section_title, None)].add(table['id'])
            table_cluster_with_similar_header_and_overlap_at_least_one_title[(cdx, -1, None, None, table_title)].add(table['id'])
    
    # len(table_cluster) == 1      : exclusion
    # 2 <= len(table_cluster) <= 5 : gold table set
    # len(table_cluster) > 5       : more clustering
    delimiters = list(table_cluster_with_similar_header_and_overlap_at_least_one_title.keys())
    for delimiter in delimiters:
        table_cluster = table_cluster_with_similar_header_and_overlap_at_least_one_title.pop(delimiter)
        if len(table_cluster) == 1:
            continue
        elif len(table_cluster) >= 2 and len(table_cluster) <= 5:
            gold_table_id_set_with_delimiter[tuple(sorted(table_cluster))].append(delimiter)
        else:
            table_cluster_with_similar_header_and_overlap_at_least_one_title[delimiter] = table_cluster
    print("Step 2 done.")

    # 3. Cluster tables w/ overlap at least two titles
    table_cluster_with_similar_header_and_overlap_at_least_two_titles = defaultdict(set)
    for delimiter, table_cluster in table_cluster_with_similar_header_and_overlap_at_least_one_title.items():
        cdx = delimiter[0] # cdx : cluster with similar header
        for table_id in table_cluster:
            table = table_lake[table_id]
            page_title = re.sub(r'\d+', '', table['metadata'].split('|')[0].lower().strip())
            section_title = re.sub(r'\d+', '', table['metadata'].split('|')[1].lower().strip())
            table_title = re.sub(r'\d+', '', table['metadata'].split('|')[2].lower().strip())
            table_cluster_with_similar_header_and_overlap_at_least_two_titles[(cdx, -1, page_title, section_title, None)].add(table['id'])
            table_cluster_with_similar_header_and_overlap_at_least_two_titles[(cdx, -1, page_title, None, table_title)].add(table['id'])
            table_cluster_with_similar_header_and_overlap_at_least_two_titles[(cdx, -1, None, section_title, table_title)].add(table['id'])
    
    # len(table_cluster) == 1      : exclusion
    # 2 <= len(table_cluster) <= 5 : gold table set
    # len(table_cluster) > 5       : more clustering
    delimiters = list(table_cluster_with_similar_header_and_overlap_at_least_two_titles.keys())
    for delimiter in delimiters:
        table_cluster = table_cluster_with_similar_header_and_overlap_at_least_two_titles.pop(delimiter)
        if len(table_cluster) == 1:
            continue
        elif len(table_cluster) >= 2 and len(table_cluster) <= 5:
            gold_table_id_set_with_delimiter[tuple(sorted(table_cluster))].append(delimiter)
        else:
            table_cluster_with_similar_header_and_overlap_at_least_two_titles[delimiter] = table_cluster
    print("Step 3 done.")

    # 4. Cluster tables w/ overlap three titles
    table_cluster_with_similar_header_and_overlap_three_titles = defaultdict(set)
    for delimiter, table_cluster in table_cluster_with_similar_header_and_overlap_at_least_two_titles.items():
        cdx = delimiter[0]
        for table_id in table_cluster:
            table = table_lake[table_id]
            page_title = re.sub(r'\d+', '', table['metadata'].split('|')[0].lower().strip())
            section_title = re.sub(r'\d+', '', table['metadata'].split('|')[1].lower().strip())
            table_title = re.sub(r'\d+', '', table['metadata'].split('|')[2].lower().strip())
            table_cluster_with_similar_header_and_overlap_three_titles[(cdx, -1, page_title, section_title, table_title)].add(table['id'])
    
    # len(table_cluster) == 1      : exclusion
    # 2 <= len(table_cluster) <= 5 : gold table set
    # len(table_cluster) > 5       : more clustering
    delimiters = list(table_cluster_with_similar_header_and_overlap_three_titles.keys())
    for delimiter in delimiters:
        table_cluster = table_cluster_with_similar_header_and_overlap_three_titles.pop(delimiter)
        if len(table_cluster) == 1:
            continue
        elif len(table_cluster) >= 2 and len(table_cluster) <= 5:
            gold_table_id_set_with_delimiter[tuple(sorted(table_cluster))].append(delimiter)
        else:
            table_cluster_with_similar_header_and_overlap_three_titles[delimiter] = table_cluster
    print("Step 4 done.")

    # 5. Include number and sort to construct gold table set
    for delimiter, table_cluster in table_cluster_with_similar_header_and_overlap_three_titles.items():
        table_cluster_by_sort_with_metadata = sorted(table_cluster, key=lambda x: table_lake[x]['metadata']) # sort by metadata, listing
        # construct : len(gold_table_set) = 3 or 4
        num_x, num_y = decompose_to_3x_4y_with_min_diff(len(table_cluster_by_sort_with_metadata))
        table_indices_list = [
            [_ * 3, _ * 3 + 1, _ * 3 + 2]
            for _ in range(num_x)
        ] + [
            [num_x * 3 + _ * 4, num_x * 3 + _ * 4 + 1, num_x * 3 + _ * 4 + 2, num_x * 3 + _ * 4 + 3]
            for _ in range(num_y)
        ]
        # split to gold table set
        for sdx, table_indices in enumerate(table_indices_list):
            gold_table_id_set_with_delimiter[tuple(sorted(
                [table_cluster_by_sort_with_metadata[tdx] for tdx in table_indices]
            ))] = [(delimiter[0], sdx, delimiter[2], delimiter[3], delimiter[4])]
    print("Step 5 done.")

    gold_table_set = [
        {
            'delimiter_list': delimiter_list,
            'gold_table_id_set': list(gold_table_id_set),
            'gold_table_metadata_set': [table_lake[table_id]['metadata'] for table_id in gold_table_id_set]
        }
        for gold_table_id_set, delimiter_list in gold_table_id_set_with_delimiter.items()
    ]

    with open('storage/gold_table_set.json', 'w') as file:
        json.dump(gold_table_set, file, indent=4)
