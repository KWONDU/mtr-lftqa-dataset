import csv
import json
import os


class Dataset():
    def __init__(self):
        self.path = None
        self.tables = None
        self.dataset = None
        self.train = None
        self.validation = None
        self.test = None
    
    def prepare(self):
        None


class TabFactDataset(Dataset):
    def __init__(self):
        self.path = 'source_data/tabfact'
        self.tables = self.__load_tables(f'{self.path}/tables')
        self.train = self.__load_data(self.path, 'train')
        self.validation = self.__load_data(self.path, 'validation')
        self.test = self.__load_data(self.path, 'test')
        
    def __load_tables(self, dir_path):
        self_tables = {}
        for table_id in os.listdir(dir_path):
            self_tables[table_id] = {'title': '', 'cell': []}
            with open(f'{dir_path}/{table_id}', 'r', encoding='utf-8') as file:
                csv_reader = csv.reader(file, delimiter='#')
                for row in csv_reader:
                    self_tables[table_id]['cell'].append(row)
        return self_tables
    
    def __load_data(self, src_file_path, split):
        self_data = {'tables': {}, 'data': {}}
        with open(f'{src_file_path}/{split}.json', 'r', encoding='utf-8') as file:
            data_dic = json.load(file)
            for table_id, data in data_dic.items():
                self.tables[table_id]['title'] = data[2]
                self_data['data'][table_id] = [statement for statement, label in zip(data[0], data[1]) if label]
        with open(f'{src_file_path}/{split}_table_ids.json', 'r', encoding='utf-8') as file:
            id_list = json.load(file)
            self_data['tables'] = {table_id: self.tables[table_id] for table_id in id_list}
        return self_data
