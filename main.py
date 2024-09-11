import logging
from tqdm import tqdm
from util import load_dataset, parser


def main(dataset_name):
    dataset = load_dataset(dataset_name=dataset_name)


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = parser()
    args, _ = parser.parse_known_args()
    logger.info(args)

    main(
        dataset_name=args.d
    )
