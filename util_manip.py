import random


def combine_with_same_title(original_tables):
    comb_tables = {}
    for table_id, content in original_tables.items():
        title = content['title']
        cell = content['cell']

        if title not in comb_tables:
            comb_tables[title] = {}
        comb_tables[title][table_id] = cell
    return comb_tables


def sample_data(data_dict, n=1):
    sample_n_data = dict(random.sample(data_dict.items(), n))
    return sample_n_data
