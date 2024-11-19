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
    total_dataset = []

    for classification in ['high_header_sim', 'low_header_sim']:
        with open(f'results/{classification}_dataset.json', 'r') as file:
            dataset = json.load(file)
    
        """
        # Table lake
        
        from utils.dataset import load_source_dataset
        from steps.regularize import regularize_source_dataset
        source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=args.d))

        for table in source_dataset.tables:
            table['source'] = 'Open-WikiTable' if classification == 'high_header_sim' else \
                'spider-tableQA' if classification == 'low_header_sim' else \
                None
            
            if table['source'] is not None:
                with open(f'../mtr-lftqa-dataset-viewer/table_lake/table_{table_id}.json', 'w') as file:
                    json.dump(table, file, indent=4)
        """

        for data in dataset:
            data['source'] = 'Open-WikiTable' if classification == 'high_header_sim' else \
                'spider-tableQA' if classification == 'low_header_sim' else \
                None
            
            if data['source'] is not None:
                total_dataset.append(data)
        
    with open(f'../mtr-lftqa-dataset-viewer/dataset.json', 'w') as file:
        json.dump(total_dataset, file, indent=4)
    
    try:
        subprocess.run(["git", "add", f"dataset.json"], cwd='../mtr-lftqa-dataset-viewer', check=True)
        subprocess.run(["git", "commit", "-m", f"modify dataset"], cwd='../mtr-lftqa-dataset-viewer', check=True)
        subprocess.run(["git", "push"], cwd='../mtr-lftqa-dataset-viewer', check=True)
    except subprocess.CalledProcessError as e:
        print(e)
