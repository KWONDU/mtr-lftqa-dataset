import argparse
import json
import subprocess
import time
from googletrans import Translator
from tqdm import tqdm


def translate_text(text: str, translator: Translator, src: str='en', dest: str='ko', retries: int=3) -> str:
    """Translate text

    [Params]
    text       : str
    translator : googletrans.Translator
    src        : str (default 'en')
    dest       : str (default 'ko')
    retries    : int (default 3)

    [Return]
    translated_text : str
    """
    for _ in range(retries):
        try:
            translated = translator.translate(text, src=src, dest=dest)
            return translated.text
        except Exception as e:
            print(f"[Translate error] {e}")
            time.sleep(2)
    return ""


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, required=True, choices=['SourceOpenWikiTable', 'SourceSpiderTableQA'], help='dataset name')

    args, _ = parser.parse_known_args()

    CLASS = {
        'SourceOpenWikiTable': 'high_header_sim',
        'SourceSpiderTableQA': 'low_header_sim'
    }

    classification = CLASS[args.d]

    # 1. Translate QA pair
    translator = Translator()

    with open(f'results/{classification}_dataset.json', 'r') as file:
        dataset = json.load(file)
    
    translated_dataset = []
    for data in tqdm(dataset):
        translated_data = data.copy()
        translated_data['translated_question'] = translate_text(text=data['question'], translator=translator)
        translated_data['translated_answer'] = translate_text(text=data['answer'], translator=translator)
        translated_dataset.append(translated_data)

    # 2. View dataset
    """
    from utils.dataset import load_source_dataset
    from steps.regularize import regularize_source_dataset
    source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=args.d))
    table_lake = {tb['id']: tb for tb in source_dataset.tables}

    for table_id, table in table_lake.items():
        with open(f'../mtr-lftqa-dataset-viewer/table_lake/{classification}_table_{table_id}.json', 'w') as file:
            json.dump(table, file, indent=4)
    """

    with open(f'../mtr-lftqa-dataset-viewer/{classification}_dataset.json', 'w') as file:
        json.dump(translated_dataset, file, indent=4)
    
    try:
        subprocess.run(["git", "add", f"{classification}_dataset.json"], cwd='../mtr-lftqa-dataset-viewer', check=True)
        subprocess.run(["git", "commit", "-m", f"modify {classification} dataset"], cwd='../mtr-lftqa-dataset-viewer', check=True)
        subprocess.run(["git", "push"], cwd='../mtr-lftqa-dataset-viewer', check=True)
    except subprocess.CalledProcessError as e:
        print(e)
