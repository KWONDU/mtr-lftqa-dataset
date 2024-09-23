from utils.format import data_format, table_format


def display_info(type, data):
    if type == 'table':
        return "\n\n".join(table_format(
            table_num=idx+1,
            metadata=table['metadata'],
            header=table['header'],
            cell=table['cell'],
            serialize=False
        ) for idx, table in enumerate(data))
    elif type == 'data':
        return "\n\n".join(data_format(
            data_num=idx+1,
            info_dict=datum,
            serialize=False
        ) for idx, datum in enumerate(data))
    else:
        return None
