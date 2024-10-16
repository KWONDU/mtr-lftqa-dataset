import json
import requests
from tqdm import tqdm


MAIN_TOPICS = [
    'Academic disciplines', 'Business', 'Communication', 'Concepts', 'Culture',
    'Economy', 'Education', 'Energy', 'Engineering', 'Entertainment',
    'Entities', 'Food and drink', 'Geography', 'Government', 'Health',
    'History', 'Human behavior', 'Humanities', 'Information', 'Internet',
    'Knowledge', 'Language', 'Law', 'Life', 'Lists',
    'Mass media', 'Mathematics', 'Military', 'Nature', 'People',
    'Philosophy', 'Politics', 'Religion', 'Science', 'Society',
    'Sports', 'Technology', 'Time', 'Universe'
]


def get_parent_categories(category):
    params = {
        'action': 'query',
        'format': 'json',
        'prop': 'categories',
        'titles': f'Category:{category}',
        'cllimit': 'max'
    }

    response = requests.get("https://en.wikipedia.org/w/api.php", params=params)
    data = response.json()

    pages = data.get('query', {}).get('pages', {})
    for _, page_data in pages.items():
        categories = page_data.get('categories', [])
        return [cat['title'].replace('Category:', '') for cat in categories]

    return []


def find_main_topic_set(category, visited=set(), main_topic_set=set()):
    if category in visited:
        return main_topic_set
    visited.add(category)

    if category in MAIN_TOPICS:
        print(category)
        main_topic_set.add(category)
    
    parent_categories = get_parent_categories(category)

    for parent_category in parent_categories:
        main_topic_set.update(find_main_topic_set(parent_category, visited=visited, main_topic_set=main_topic_set))
    
    return main_topic_set


if __name__ == '__main__':
    with open('storage/wikipedia_content_related_category_set_for_each_page.json', 'r') as file:
        wiki_category_set_per_page = json.load(file)
    
    category_list = [category for _, category_set in wiki_category_set_per_page.items() for category in category_set][:10]
    main_topic_set_per_category = {
        category: find_main_topic_set(category)
        for category in tqdm(category_list, total=len(category_list))
    }

    with open('storage/main_topic_set_for_each_category.json', 'w') as file:
        print(main_topic_set_per_category)
        json.dump(main_topic_set_per_category, file, indent=4)
