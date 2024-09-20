def table_format(table_num, metadata, header, cell, serialize=False):
    header = header or []
    cell = cell or [['' for _ in range(len(header))]]
    
    col_widths = [max(len(str(item)) for item in col) for col in zip(*([header] + (cell)))]

    serialize_header = " | ".join(f"{header[i]:<{col_widths[i]}}" for i in range(len(header)))
    if serialize: # serialization
        sep_token = ""
    else:
        sep_token = "-" * len(serialize_header)

    serialize_cell = sep_token
    for i, row in enumerate(cell):
        serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
        if serialize: # serialization
            serialize_cell = " | ".join([serialize_cell, f"row {i+1}: {serialize_row}"])
        else:
            serialize_cell = "\n".join([serialize_cell, serialize_row])
    
    display_table = f"Table #{table_num} information"
    display_table = "\n".join([display_table, f"metadata: {metadata}" if metadata else ""])
    if header == [] and cell == [['' for _ in range(len(header))]]:
        pass
    elif header != [] and cell == [['' for _ in range(len(header))]]:
        display_table = "\n".join([display_table, f"header: {serialize_header}"])
    else:
        display_table =  "\n".join([display_table, "full table:"])
        if serialize: # serialization
            display_table = "\n".join([display_table, f"col: {serialize_header}" if serialize_header else ""])
            display_table = "".join([display_table, serialize_cell])
        else:
            display_table = "\n".join([display_table, serialize_header if serialize_header else ""])
            display_table = "\n".join([display_table, serialize_cell])

    return display_table
