import json
from datasets import load_dataset


class Dataset():
    def __init__(self):
        self.dataset = None
        self.train = None
        self.validation = None
        self.test = None
        self.tables = None
    
    def prepare(self):
        None


class SpiderDataset(Dataset):
    def __init__(self):
        super().__init__()
        self.dataset = load_dataset('xlangai/spider')
        self.train = self.dataset['train']
        self.validation = self.dataset['validation']
        with open('source_data/spider/tables.json', 'r') as file:
            self.tables = json.load(file)

    def prepare(self):
        None
