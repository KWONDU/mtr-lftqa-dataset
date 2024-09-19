from utils.format import data_format, table_format


def display_data(data_list):
    return "\n\n".join(data_format(
        data_num=data_idx+1,
        question=data['question'],
        sql=next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type=='SQL'),
        sub_table=next(answer for answer, answer_type in zip(data['answer'], data['answer_type']) if answer_type=='table')
    ) for data_idx, data in enumerate(data_list))


def display_table(table_dict, gold_table_set):
    return "\n\n".join(table_format(
        table_num=table_idx+1,
        metadata=table_dict[gold_table_id]['metadata'],
        header=table_dict[gold_table_id]['header'],
        cell=table_dict[gold_table_id]['cell']
    ) for table_idx, gold_table_id in enumerate(gold_table_set))
