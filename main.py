import logging
from tqdm import tqdm
from util import load_dataset, parser
from util_llm import generate_long_form_answer
from util_manip import combine_with_same_title, sample_data


def main(dataset_name):
    dataset_class = load_dataset(dataset_name)
    logger.info(dataset_class)

    original_tables = dataset_class.full['tables']
    comb_tables = combine_with_same_title(original_tables)

    # single page - multi tables
    comb_tables = {key: value for key, value in comb_tables.items() if len(value) > 1}
    sample_comb_tables = sample_data(comb_tables, n=10)

    input_data = {}
    for title, content in sample_comb_tables.items():
        input_data[title] = []
        for table_id, cell in content.items():
            table_header = cell[0]
            statements = dataset_class.full['data'][table_id]
            input_data[title].append({'header': table_header, 'statements': statements})
    
    # save to file
    file_text = ""
    for title, data in tqdm(input_data.items()):
        sys_pmt, usr_pmt, ans = generate_long_form_answer(title, data)
        file_text = file_text + (
            "### System prompt ###\n" +
            sys_pmt +
            "\n### User prompt ###\n" +
            usr_pmt +
            "\n### Long-form statement ###\n" +
            ans +
            "\n\n===\n"
        )
    
    with open(f'{dataset_name.lower()}-lf-answer.txt', 'w') as file:
        file.write(file_text)


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
