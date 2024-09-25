import json
import os
import requests
import sys
import wikipediaapi
from tqdm import tqdm


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    return load_dataset


if __name__ == '__main__':
    load_dataset = import_utils()

    tabfact = load_dataset(dataset_name='TabFact')
    table_id_key_page_title_value_dict = {table['id']: table['metadata'] for table in tabfact.tables}

    wikipedia_category_set_for_each_table_dict = {}

    wiki = wikipediaapi.Wikipedia(user_agent='MyScript/1.0 (Contact: email@example.com)')

    for idx, (table_id, page_title) in tqdm(enumerate(table_id_key_page_title_value_dict.items()), total=len(table_id_key_page_title_value_dict)):
        page = wiki.page(page_title)

        if not page.exists():
            url = f"https://en.wikipedia.org/w/api.php"
            params = {
                'action': 'query',
                'list': 'search',
                'srsearch': page_title,
                'format': 'json'
            }

            response = requests.get(url, params=params)
            search_results = response.json()['query']['search']

            if not search_results:
                print(idx, page_title, response)
                continue
                
            similar_page_title = search_results[0]['title']
            page = wiki.page(similar_page_title)
        
        category_set = [category[len('Category:'):] for category in page.categories]

        wikipedia_category_set_for_each_table_dict[table_id] = category_set
    
    with open('storage/wikipedia_category_set_for_each_table.json', 'w') as file:
        json.dump(wikipedia_category_set_for_each_table_dict, file)
