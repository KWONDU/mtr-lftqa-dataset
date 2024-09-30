def data_format(data_num, data, type):
    if type == 'sentence':
        display_data = f"Sentence #{data_num}:" if data_num >= 0 else ""
        display_data = "\n".join([display_data, data])
    
    elif type == 'document':
        display_data = "Document:"
        display_data = "\n".join([display_data, data])

    return display_data
