def data_format(data_num, info_dict, serialize=False):
    display_data = f"Data #{data_num} information"

    for key, value in info_dict.items():
        # sql_query, sql_extraction, statement
        if key == 'sql_query':
            display_data = "\n".join([display_data, f"SQL query: {value}"])
        
        elif key == 'statement':
            display_data = "\n".join([display_data, f"Entailed statement: {value}"])

        elif key == 'sql_extraction':
            sub_table = value or {'header': [], 'cell': [[]]}
            sub_header = sub_table['header'] or []
            sub_cell = sub_table['cell'] or [[]]

            col_widths = [max(len(str(item)) for item in col) for col in zip(*([sub_header] + (sub_cell)))]

            serialize_header = " | ".join(f"{sub_header[i]:<{col_widths[i]}}" for i in range(len(sub_header)))
            if serialize: # serialization
                sep_token = ""
            else:
                sep_token = "-" * len(serialize_header)

            serialize_cell = sep_token
            for i, row in enumerate(sub_cell):
                serialize_row = " | ".join(f"{'' if row[i] is None else row[i]:<{col_widths[i]}}" for i in range(len(row)))
                if serialize: # serialization
                    serialize_cell = " | ".join([serialize_cell, f"row {i+1}: {serialize_row}"])
                else:
                    serialize_cell = "\n".join([serialize_cell, serialize_row])
    
            if sub_header == [] and sub_cell == [[]]:
                pass
            elif sub_header != [] and sub_cell == [[]]:
                display_data = "\n".join([display_data, f"SQL extraction header: {serialize_header}"])
            else:
                display_data = "\n".join([display_data, "SQL extraction:"])
                if serialize: # serialization
                    display_data = "\n".join([display_data, f"col: {serialize_header}" if serialize_header else ""])
                    display_data = "".join([display_data, serialize_cell])
                else:
                    display_data = "\n".join([display_data, serialize_header if serialize_header else ""])
                    display_data = "\n".join([display_data, serialize_cell])
        
        else:
            continue

    return display_data
