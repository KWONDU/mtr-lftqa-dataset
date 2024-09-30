import argparse
import json
import os
import sys


def import_utils():
    parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    sys.path.insert(0, parent_dir)
    from utils.dataset import load_dataset
    return load_dataset


if __name__ == '__main__':
    load_dataset = import_utils()
    tabfact = load_dataset(dataset_name='TabFact')

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', type=str, required=True, help='file name')

    args, _ = parser.parse_known_args()

    try:
        with open(f'storage/{args.f}.json', 'r') as file:
            json_file = json.load(file)
    except:
        print(f"[Error] no file named: {args.f}.json")
        exit()
    
    result = ""

    if args.f == 'generated_coarse_grained_topic_set_for_each_page':
        with open('storage/wikipedia_category_set_for_each_page.json', 'r') as file:
            category_set_per_page = json.load(file)

        for page_title, full_topic_set in json_file.items():
            result += f"Page title: {page_title}\n"
            result += f"Category set: [{', '.join(category_set_per_page[page_title])}]"
            result += f"Topic set:\n"
            for level, topic_set in full_topic_set.items():
                result += f">\t{level}: [{', '.join(topic_set)}]\n"
            result += "###\n"
    
    elif args.f == 'generated_document_for_each_table':
        for table_id, document in json_file.items():
            result += f"Table id: {table_id}\n"
            result += f"Entailed statements:\n"
            for data in tabfact[:]:
                if next(_ for _ in data['gold_tables']) == table_id and data['answer_type']:
                    result += f"-\t{data['answer']}\n"
            result += f"Document: {document}\n"
            result += "###\n"
    
    elif args.f == 'generated_fine_grained_topic_set_for_each_table':
        with open('storage/generated_document_for_each_table.json', 'r') as file:
            document_per_table = json.load(file)

        for table_id, topic_set in json_file.items():
            result += f"Table id: {table_id}\n"
            result += f"Entailed statements:\n"
            for data in tabfact[:]:
                if next(_ for _ in data['gold_tables']) == table_id and data['answer_type']:
                    result += f"-\t{data['answer']}\n"
            result += f"Document: {document_per_table[table_id]}\n"
            result += f"Topic set:\n"
            for topic in topic_set:
                result += f"-\t{topic}\n"
            result += "###\n"

    with open(f'buffer/{args.f}_visualization.txt', 'w') as file:
        file.write(result)
