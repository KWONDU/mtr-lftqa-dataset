from typing import Any, List, Optional


def table_serialization(table_num: int, metadata: Optional[str]=None, header: Optional[List[str]]=None, cell: Optional[List[List[Any]]]=None) -> str:
    """Serialize table in specified format

    e.g.
    Table 1 [title]: metadata [header]: col1 | col2 ... [row1]: cell1 | cell2 ...

    [Params]
    table_num : int
    metadata     : str
    header    : List[str]
    cell      : List[List[Any]]

    [Return]
    serialization : str
    """
    serialization = f"Table {table_num}" if table_num > 0 else ""
    
    serialization = " ".join([serialization, f"[title]: {metadata}"]) if metadata else ""
    
    if header is not None:
        serialize_header = " | ".join([str(_) for _ in header])
        serialization = " ".join([serialization, f"[header]: {serialize_header}"])

    if cell is not None:  
        for i, row in enumerate(cell):
            serialize_row = " | ".join([str(_) for _ in row])
            serialization = " ".join([serialization, f"[row {i+1}]: {serialize_row}"])
    
    serialization = serialization.strip()
    
    return serialization
