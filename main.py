import logging
from tqdm import tqdm
from util import load_dataset, parser
from util_llm import generate_long_form_answer
from util_manip import sample_data


def main(dataset_name):
    dataset_class = load_dataset(dataset_name)
    logger.info(dataset_class)


    tables = dataset_class.test['tables']
    sample_data_dict = sample_data(dataset_class.test['data'], n=10)
    long_form_answer_list = []
    for table_id, data in tqdm(sample_data_dict.items()):
        long_form_answer_list.append(generate_long_form_answer(tables[table_id], data))
    
    with open('result.txt', 'w') as file:
        data = ""
        i = 0
        for (_, sf_as), lf_a in zip(sample_data_dict.items(), long_form_answer_list):
            data = data + f"### {i}th ###\n"
            data = data + ">  Short-form answers\n"
            for sf_a in sf_as:
                data = data + sf_a + "\n"
            data = data + ">  Long-form answer\n"
            data = data + lf_a + "\n\n"
            i = i + 1
        file.write(data)


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
