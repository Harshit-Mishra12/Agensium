import pandas as pd
import io
from fastapi import HTTPException

def _profile_dataframe(df: pd.DataFrame):
    """
    Helper function to generate a schema summary for a single DataFrame.
    """
    # Handle empty dataframes
    if df.empty:
        return {
            "summary_table": [],
            "total_rows": 0,
            "message": "Sheet is empty."
        }
        
    schema_summary = []

    for col in df.columns:
        # 1. Calculate Null Percentage
        null_count = df[col].isnull().sum()
        total_count = len(df)
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

        # 2. Get Distinct Count
        distinct_count = df[col].nunique()

        # 3. Get Top Values (we'll take the top 3)
        top_values = df[col].value_counts().nlargest(3).index.tolist()
        top_values_str = ', '.join(map(str, top_values))

        # 4. Get Data Type (with date detection)
        col_type = str(df[col].dtype)
        if 'object' in col_type:
            try:
                pd.to_datetime(df[col], errors='raise')
                data_type = 'Date'
            except (ValueError, TypeError):
                data_type = 'Text'
        elif 'int' in col_type:
            data_type = 'Integer'
        elif 'float' in col_type:
            data_type = 'Float'
        elif 'datetime' in col_type:
            data_type = 'Date'
        else:
            data_type = col_type

        schema_summary.append({
            "field": col,
            "data_type": data_type,
            "null": f"{null_percentage:.1f}%",
            "distinct_count": distinct_count,
            "top_values": top_values_str + ('…' if distinct_count > 3 else '')
        })

    return {
        "summary_table": schema_summary,
        "total_rows": len(df)
    }

def scan_schema(file_contents: bytes, filename: str):
    """
    Scans a CSV or Excel file and returns a schema profile.
    Handles multiple sheets in Excel files.
    """
    file_extension = filename.split('.')[-1].lower()
    all_sheets_summary = {}

    try:
        if file_extension == 'csv':
            df = pd.read_csv(io.BytesIO(file_contents))
            sheet_name = filename.rsplit('.', 1)[0]
            all_sheets_summary[sheet_name] = _profile_dataframe(df)

        elif file_extension in ['xlsx', 'xls']:
            # Load all sheets by setting sheet_name=None
            xls_sheets = pd.read_excel(io.BytesIO(file_contents), sheet_name=None)
            
            if not xls_sheets:
                raise ValueError("The Excel file appears to be empty or has no sheets.")

            for sheet_name, df in xls_sheets.items():
                all_sheets_summary[sheet_name] = _profile_dataframe(df)
        
        else:
            # This case should be caught by the router, but it's good practice to have it.
            raise HTTPException(status_code=400, detail=f"Unsupported file format: {file_extension}")

        return all_sheets_summary

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to process the file '{filename}'. Error: {str(e)}")

