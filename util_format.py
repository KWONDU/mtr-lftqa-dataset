def data_format(data_num, question, sql, sub_table):
    sub_table = sub_table or {'header': [], 'cell': [[]]}

    sub_header, sub_cell = sub_table['header'], sub_table['cell']
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([sub_header] + (sub_cell)))]

    serialize_header = " | ".join(f"{sub_header[i]:<{col_widths[i]}}" for i in range(len(sub_header)))
    sep_token = "-" * len(serialize_header)

    serialize_cell = ""
    for row in sub_cell:
        serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
        serialize_cell = serialize_cell + serialize_row + "\n"

    display_data = f"Data #{data_num} information\n"
    display_data += f"question: {question}\n" if question else ""
    display_data += f"SQL query: {sql}\n" if sql else ""
    if sub_header == [] and sub_cell == [[]]:
        pass
    elif sub_header != [] and sub_cell == [[]]:
        display_data += f"header: {serialize_header}\n"
    else:
        display_data += (f"{serialize_header}\n" if serialize_header else "")
        display_data += (f"{sep_token}\n" if sep_token else "")
        display_data += f"{serialize_cell}"
    display_data += "\n"

    return display_data


def table_format(table_num, metadata, header, cell):
    header = header or []
    cell = cell or [['' for _ in range(len(header))]]
    
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([header] + (cell)))]

    serialize_header = " | ".join(f"{header[i]:<{col_widths[i]}}" for i in range(len(header)))
    sep_token = "-" * len(serialize_header)

    serialize_cell = ""
    for row in cell:
        serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
        serialize_cell = serialize_cell + serialize_row + "\n"
    
    display_table = f"Table #{table_num} information\n"
    display_table += f"metadata: {metadata}\n" if metadata else ""
    if header == [] and cell == [[]]:
        pass
    elif header != [] and cell == [[]]:
        display_table += f"header: {serialize_header}\n"
    else:
        display_table += (f"{serialize_header}\n" if serialize_header else "")
        display_table += (f"{sep_token}\n" if sep_token else "")
        display_table += f"{serialize_cell}"
    display_table += "\n"

    return display_table
