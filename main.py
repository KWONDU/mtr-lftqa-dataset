import argparse
import logging
import random
from steps.regularize import regularize_source_dataset
from utils.dataset import load_source_dataset


if __name__ == '__main__':
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(logging.StreamHandler())

    parser = argparse.ArgumentParser()
    parser.add_argument('-d', type=str, required=True, choices=['SourceOpenWikiTable', 'SourceSpiderTableQA'], help='dataset name')
    parser.add_argument('-n', type=int, required=True, help='number of sampled data')

    args, _ = parser.parse_known_args()
    logger.info(args)

    """
    api_key = '___YOUR_OPENAI_API_KEY___'
    from utils.openai import add_openai_api_key
    add_openai_api_key(api_key=api_key)
    """

    source_dataset = regularize_source_dataset(source_dataset=load_source_dataset(dataset_name=args.d))

    random.seed(4242)
    table_lake = {tb['id']: tb for tb in source_dataset.tables}
    instance_set = random.sample(source_dataset[:], args.n)
    MODEL_NAME = 'gpt-4o-mini'
    BATCH_SIZE = 50

    if args.d == 'SourceOpenWikiTable':
        from annotate_high_header_sim import main
    elif args.d == 'SourceSpiderTableQA':
        from annotate_low_header_sim import main
    
    FLAG = [None, False, None, False, False, False, False, False]

    main(
        table_lake=table_lake,
        instance_set=instance_set,
        model_name=MODEL_NAME,
        batch_size=BATCH_SIZE,
        flag=FLAG,
        logger=logger
    )
