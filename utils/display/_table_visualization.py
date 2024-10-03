def table_visualization(table_num, metadata, header, cell):
    visualization = f"Table {table_num}:" if table_num >= 0 else ""
    visualization = "\n".join([visualization, f"metadata: {metadata}"]) if metadata else ""

    header = header or []
    cell = cell or [['' for _ in range(len(header))]]

    col_widths = [len(str(header[i])) for i in range(len(header))]

    for row in cell:
        for i in range(len(header)):
            col_widths[i] = max(col_widths[i], len(str(row[i])))

    serialize_header = " | ".join(f"{header[i]:<{col_widths[i]}}" for i in range(len(header)))
    sep_token = "-" * len(serialize_header)

    serialize_cell = sep_token
    for _, row in enumerate(cell):
        serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
        serialize_cell = "\n".join([serialize_cell, serialize_row])
    
    if header == [] and cell == [['' for _ in range(len(header))]]:
        pass
    elif header != [] and cell == [['' for _ in range(len(header))]]:
        visualization = "\n".join([visualization, f"header: {serialize_header}"])
    else:
        visualization =  "\n".join([visualization, "full table:"]) if visualization != "" else ""
        visualization = "\n".join([visualization, serialize_header if serialize_header else ""])
        visualization = "\n".join([visualization, serialize_cell])
    
    return visualization
