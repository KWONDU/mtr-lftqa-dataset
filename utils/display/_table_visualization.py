from typing import Any, List, Optional


def table_visualization(table_num: int, title: Optional[str]=None, header: Optional[List[str]]=None, cell: Optional[List[List[Any]]]=None) -> str:
    """Visualize table in specified format

    e.g.
    Table 1:
    title: title
    col1  | col2  | ...
    -------------------
    cell1 | cell2 | ...

    [Params]
    table_num : int
    title     : str
    header    : List[str]
    cell      : List[List[Any]]

    [Return]
    visualization : str
    """
    visualization = f"Table {table_num}:" if table_num >= 0 else ""
    visualization = "\n".join([visualization, f"title: {title}"]) if title else ""

    header = header or []
    cell = cell or [['' for _ in range(len(header))]]

    col_widths = [len(str(header[i])) for i in range(len(header))]

    for row in cell:
        for i in range(len(header)):
            col_widths[i] = max(col_widths[i], len(str(row[i])))

    serialize_header = " | ".join(f"{str(header[i]):<{col_widths[i]}}" for i in range(len(header)))
    sep_token = "-" * len(serialize_header)

    serialize_cell = sep_token
    for _, row in enumerate(cell):
        serialize_row = " | ".join(f"{'' if row[i] is None else str(row[i]):<{col_widths[i]}}" for i in range(len(row)))
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
