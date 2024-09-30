import aiohttp
import asyncio
import json
from asyncio import Semaphore
from collections import defaultdict
from tqdm.asyncio import tqdm_asyncio


async def get_parent_categories(session, category):
    params = {
        'action': 'query',
        'format': 'json',
        'titles': f'Category:{category}',
        'prop': 'categories',
        'cllimit': 'max'
    }

    async with semaphore:
        async with session.get("https://en.wikipedia.org/w/api.php", params=params) as response:
            response_json = await response.json()
            pages = response_json.get('query', {}).get('pages', {})

            parent_categories = []
            for _, page_data in pages.items():
                if 'categories' in page_data:
                    parent_categories = [cat['title'][len('Category:'):] for cat in page_data['categories']]
            
            return parent_categories


async def find_subject_set_per_category(session, category, subject_list, visited=set()):
    if category in visited:
        return set()
    
    visited.add(category)
    parent_categories = await get_parent_categories(session=session, category=category)

    matched_subject_set = set()

    for par_cat in parent_categories:
        if par_cat in subject_list:
            matched_subject_set.add(par_cat)
        else:
            matched_subject_set_from_parent_category = await find_subject_set_per_category(
                session=session,
                category=par_cat,
                subject_list=subject_list,
                visited=visited
            )
            matched_subject_set.update(matched_subject_set_from_parent_category)

    return matched_subject_set


async def find_subject_set_with_cataegory_list(category_list, subject_list):
    async with aiohttp.ClientSession() as session:
        tasks = [
            find_subject_set_per_category(
                session=session,
                category=category,
                subject_list=subject_list
            ) for category in category_list
        ]

        results = await tqdm_asyncio.gather(*tasks, total=len(tasks))

        return {
            category: results[idx]
            for idx, category in enumerate(category_list)
        }


async def main():
    with open('storage/wikipedia_category_set_for_each_page.json', 'r') as file:
        category_set_per_page = json.load(file)

    category_set_per_page = {k: v for i, (k, v) in enumerate(category_set_per_page.items()) if i < 10}
    
    match_category_to_page_title = defaultdict(list)
    for page_title, category_set in category_set_per_page.items():
        for category in category_set:
            match_category_to_page_title[category].append(page_title)
    
    match_category_to_subject_set = await find_subject_set_with_cataegory_list(
        category_list=match_category_to_page_title.keys(),
        subject_list=SUBJECT_LIST
        )
    
    subject_set_per_page = defaultdict(set)
    for page_title, category_set in category_set_per_page.items():
        for category in category_set:
            subject_set_per_page[page_title].update(match_category_to_subject_set[category])
        subject_set_per_page[page_title] = list(subject_set_per_page[page_title])
    
    with open('storage/wikipedia_subject_set_for_each_page.json', 'w') as file:
        json.dump(subject_set_per_page, file, indent=4)


if __name__ == '__main__':
    semaphore = Semaphore(10)
    SUBJECT_LIST = ['Research', 'Library Science',
                    'Culture', 'The arts',
                    'Geography', 'Places',
                    'Health', 'Self-care', 'Health care occupations',
                    'History', 'Events',
                    'Human activities',
                    'Formal sciences', 'Mathematics', 'Logic', 'Mathematical sciences',
                    'Science', 'Natural science', 'Nature',
                    'People', 'Personal life', 'Self', 'Surnames',
                    'Philosophy', 'Thought',
                    'Religion', 'Blief',
                    'Society', 'Social sciences',
                    'Technology', 'Applied sciences'
                    ]

    asyncio.run(main())
