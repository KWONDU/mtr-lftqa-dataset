import aiohttp
import asyncio
import json
import os
import sys
import wikipediaapi
from asyncio import Semaphore
from tqdm.asyncio import tqdm_asyncio


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    return load_dataset


async def fetch_wikipedia_page(session, wiki, page_title, wikipedia_searched_page_title_dict, wikipedia_category_set_for_each_page_dict):
    page = wiki.page(page_title)

    if not page.exists():
        url = f"https://en.wikipedia.org/w/api.php"
        params = {
            'action': 'query',
            'list': 'search',
            'srsearch': page_title,
            'format': 'json'
        }

        async with session.get(url, params=params) as response:
            search_data = await response.json()
            search_results = search_data['query']['search']

            if not search_results:
                print(f"[Error] No page title: {page_title}")
                return

            similar_page_title = search_results[0]['title']
            page = wiki.page(similar_page_title)
            wikipedia_searched_page_title_dict[page_title] = similar_page_title
    
    else:
        wikipedia_searched_page_title_dict[page_title] = page_title
    
    category_set = [category[len('Category:'):] for category in page.categories]
    wikipedia_category_set_for_each_page_dict[page_title] = category_set


async def main():
    load_dataset = import_utils()

    tabfact = load_dataset(dataset_name='TabFact')
    page_title_list = list(set(table['metadata'] for table in tabfact.tables))

    wikipedia_searched_page_title_dict = {}
    wikipedia_category_set_for_each_page_dict = {}

    wiki = wikipediaapi.Wikipedia(user_agent='MyScript/1.0 (Contact: email@example.com)')

    semaphore = Semaphore(10)

    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_wikipedia_page(
                session=session,
                wiki=wiki,
                page_title=page_title,
                wikipedia_searched_page_title_dict=wikipedia_searched_page_title_dict,
                wikipedia_category_set_for_each_page_dict=wikipedia_category_set_for_each_page_dict,
                semaphore=semaphore
            ) for page_title in page_title_list
        ]

        for task in tqdm_asyncio.as_completed(tasks):
            await task
    
    with open('storage/wikipedia_category_set_for_each_page.json', 'w') as file:
        json.dump(sorted(wikipedia_category_set_for_each_page_dict.items()), file, indent=4)

    with open('storage/wikipedia_searched_page_title.json', 'w') as file:
        json.dump(sorted(wikipedia_searched_page_title_dict.items()), file, indent=4)


if __name__ == '__main__':
    asyncio.run(main())
