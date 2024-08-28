import logging
from util import load_dataset, parser
from util_llm import short2long


def main(dataset_name):
    dataset_class = load_dataset(dataset_name)
    dataset_class.prepare()
    logger.info(dataset_class.dataset)

    s_questions = [data['question'] for data in dataset_class.validation if data['db_id'] == "real_estate_properties"]
    print('\n'.join(s_questions), len(s_questions), sep='\n')
    l_question = short2long(s_questions)
    print(l_question)


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
