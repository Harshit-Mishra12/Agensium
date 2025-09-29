import pandas as pd

def scan_schema(dataset: list[dict]):
    """
    Example agent: scan dataset schema
    """
    df = pd.DataFrame(dataset)
    fields = {col: str(df[col].dtype) for col in df.columns}
    return {"fields": fields, "row_count": len(df)}


# [
#   {"id": 1, "name": "Alice", "age": 30},
#   {"id": 2, "name": "Bob", "age": 25},
#   {"id": 3, "name": "Charlie", "age": 35}
# ]
