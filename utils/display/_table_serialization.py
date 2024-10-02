def table_serialization(table_num, metadata, header, cell):
    serialization = f"Table {table_num}" if table_num >= 0 else ""
    
    serialization = " ".join([serialization, f"[table name]: {metadata}"]) if metadata else ""

    serialize_header = " | ".join([_ for _ in header])
    serialization = " ".join([serialization, f"[header]: {serialize_header}"])

    for i, row in enumerate(cell):
        serialize_row = " | ".join([_ for _ in row])
        serialization = " ".join([serialization, f"[row {i+1}]: {serialize_row}"])
    
    return serialization
