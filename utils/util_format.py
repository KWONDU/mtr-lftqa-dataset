def data_format(data_num, question, sql, sub_table):
    sub_table = sub_table or {'header': [], 'cell': [[]]}

    sub_header, sub_cell = sub_table['header'], sub_table['cell']
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([sub_header] + (sub_cell)))]

    serialize_header = " | ".join(f"{sub_header[i]:<{col_widths[i]}}" for i in range(len(sub_header)))
    sep_token = "-" * len(serialize_header)

    serialize_cell = sep_token
    for row in sub_cell:
        serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
        serialize_cell = "\n".join([serialize_cell, serialize_row])

    display_data = f"Data #{data_num} information"
    display_data = "\n".join([display_data, f"question: {question}" if question else ""])
    display_data = "\n".join([display_data, f"SQL query: {sql}" if sql else ""])
    if sub_header == [] and sub_cell == [[]]:
        pass
    elif sub_header != [] and sub_cell == [[]]:
        display_data = "\n".join([display_data, f"header: {serialize_header}"])
    else:
        display_data = "\n".join([display_data, "SQL extraction:"])
        display_data = "\n".join([display_data, serialize_header if serialize_header else ""])
        display_data = "\n".join([display_data, serialize_cell])

    return display_data


def table_format(table_num, metadata, header, cell):
    header = header or []
    cell = cell or [['' for _ in range(len(header))]]
    
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([header] + (cell)))]

    serialize_header = " | ".join(f"{header[i]:<{col_widths[i]}}" for i in range(len(header)))
    sep_token = "-" * len(serialize_header)

    serialize_cell = sep_token
    for row in cell:
        serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
        serialize_cell = "\n".join([serialize_cell, serialize_row])
    
    display_table = f"Table #{table_num} information"
    display_table = "\n".join([display_table, f"metadata: {metadata}" if metadata else ""])
    if header == [] and cell == [['' for _ in range(len(header))]]:
        pass
    elif header != [] and cell == [['' for _ in range(len(header))]]:
        display_table = "\n".join([display_table, f"header: {serialize_header}"])
    else:
        display_table =  "\n".join([display_table, "full table:"])
        display_table = "\n".join([display_table, serialize_header if serialize_header else ""])
        display_table = "\n".join([display_table, f"{serialize_cell}"])

    return display_table
