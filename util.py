import argparse
import glob
import logging
from util_dill import load_large_object


def load_dataset(dataset_name):
    try:
        processed_dataset_name = dataset_name.replace('-', '')
        dataset = load_large_object(
            glob.glob(f'dataset/dump/{processed_dataset_name.lower()}*.pkl'),
            'dataset',
            f'load_{processed_dataset_name.lower()}'
            )
    except Exception as e:
        logger.critical(e)
    return dataset


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default='MultiTabQA', help='dataset name')
    parser.add_argument('-n', type=int, default=10, help="# of sampled data")
    return parser


if __name__ == 'util':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler())
