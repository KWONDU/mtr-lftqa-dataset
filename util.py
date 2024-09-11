import argparse
import dill
import importlib
import logging

"""
def load_dataset(dataset_name):
    try:
        processed_dataset_name = dataset_name.replace('-', '')
        with open(f'dataset/dump/{processed_dataset_name.lower()}.dill', 'rb') as file:
            __coarse_module = importlib.import_module('dataset.dataset_template')
            __fine_module = importlib.import_module(f'dataset.load_{processed_dataset_name.lower()}')
            dataset = dill.load(file)
    except Exception as e:
        logger.critical(e)
    return dataset
"""

def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default='', help="dataset name")
    return parser


if __name__ == 'util':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler())
