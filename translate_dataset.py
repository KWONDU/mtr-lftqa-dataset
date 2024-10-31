import argparse
import json
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
    translator = Translator()

    with open(f'results/{classification}_dataset.json', 'r') as file:
        dataset = json.load(file)
    
    translated_dataset = []
    for data in tqdm(dataset):
        translated_data = data.copy()
        translated_data['translated_question'] = translate_text(text=data['question'], translator=translator)
        translated_data['translated_answer'] = translate_text(text=data['answer'], translator=translator)
        translated_dataset.append(translated_data)
    
    with open(f'results/{classification}_dataset.json', 'w') as file:
        json.dump(translated_dataset, file, indent=4)
