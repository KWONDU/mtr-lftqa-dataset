import json
import requests
import time
from bs4 import BeautifulSoup
from tqdm import tqdm


def get_category_set(page_title):
    wikipedia_url = f"https://en.wikipedia.org/wiki/{page_title}"

    try:
        response = requests.get(wikipedia_url)
        soup = BeautifulSoup(response.text, 'html.parser')

        categories = [
            cat.text
            for cat in soup.select('#mw-normal-catlinks ul li a')
        ]
    
    except:
        categories = []
    
    time.sleep(2) # delay

    return categories


if __name__ == '__main__':
    with open('storage/matched_wikipedia_page_title.json', 'r') as file:
        matched_page_title_dict = json.load(file)
    
    # Modify wrong page titles
    with open('storage/modified_page_title_set_with_human_annotation.json', 'r') as file:
        modified_page_title_set_with_human_annotation = json.load(file)
    
    for page_title, modified_page_title in modified_page_title_set_with_human_annotation.items():
        matched_page_title_dict[page_title] = modified_page_title
    # Finish reflect modifications

    category_set_per_page_title = dict()
    for page_title, wiki_page_title in tqdm(matched_page_title_dict.items(), total=len(matched_page_title_dict)):
        category_set = get_category_set(page_title=wiki_page_title)
        category_set_per_page_title[page_title] = category_set
        
        with open(f'storage/wikipedia_content_related_category_set_for_each_page.json', 'w') as file:
            json.dump(category_set_per_page_title, file, indent=4) # update
