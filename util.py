import argparse
import importlib
import logging


def load_dataset(dataset_name):
    try:
        module = importlib.import_module('dataset')
        dataset_class = getattr(module, f'{dataset_name}Dataset')()
    except Exception as e:
        logger.critical(e)

    return dataset_class


def parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, default='', help="dataset name")

    return parser


if __name__ == 'util':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.WARNING)
    logger.addHandler(logging.StreamHandler())
