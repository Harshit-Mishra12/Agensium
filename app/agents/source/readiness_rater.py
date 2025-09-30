import pandas as pd
import io
import sqlparse
from fastapi import HTTPException

def _read_data_and_calculate_score(source_type: str, file_contents: bytes, filename: str):
    """
    Reads data from an uploaded file and calculates the readiness score.
    """
    try:
        # Handle SQL files separately as they contain schema, not data
        if source_type == 'sql':
            sql_script = file_contents.decode('utf-8')
            parsed = sqlparse.parse(sql_script)
            
            schema_health_score = 100
            if not parsed or not any(isinstance(t, sqlparse.sql.Statement) for t in parsed):
                schema_health_score = 0 # Penalize heavily if the SQL is invalid

            # A simple heuristic: Penalize if there are no CREATE TABLE statements
            if 'create table' not in sql_script.lower():
                schema_health_score -= 50

            readiness_score = {
                "overall": round(schema_health_score * 0.2), # Only schema health contributes
                "completeness": 100, # Assume 100 as there's no data to have nulls
                "consistency": 100,  # Assume 100 as there's no data to have duplicates
                "schema_health": round(schema_health_score)
            }
            return readiness_score, 0 # 0 rows analyzed

        # Process data-containing files (CSV, Excel, etc.)
        df = _read_data_from_file(source_type, file_contents)
        readiness_score = _calculate_readiness_score(df)
        return readiness_score, len(df)

    except (IOError, ValueError) as e:
         raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An unexpected error occurred while processing '{filename}': {str(e)}")


def _read_data_from_file(source_type: str, file_contents: bytes):
    """
    Reads data from various file formats into a DataFrame.
    """
    if source_type == 'csv':
        return pd.read_csv(io.BytesIO(file_contents))
    elif source_type in ['xlsx', 'xls']:
        return pd.read_excel(io.BytesIO(file_contents), sheet_name=0)
    elif source_type == 'json':
        return pd.read_json(io.BytesIO(file_contents))
    elif source_type == 'parquet':
        return pd.read_parquet(io.BytesIO(file_contents))
    else:
        raise ValueError("Unsupported file type")


def _calculate_readiness_score(df: pd.DataFrame):
    """
    Calculates the readiness score for a single DataFrame.
    """
    if df.empty:
        return {
            "overall": 0, "completeness": 0, "consistency": 0, "schema_health": 0,
            "message": "Dataset is empty."
        }

    # 1. Completeness Score (based on nulls)
    total_cells = df.size
    null_cells = df.isnull().sum().sum()
    completeness_score = max(0, 100 - (null_cells / total_cells * 100))

    # 2. Consistency Score (based on duplicate rows)
    duplicate_rows = df.duplicated().sum()
    total_rows = len(df)
    consistency_score = max(0, 100 - (duplicate_rows / total_rows * 100))

    # 3. Schema Health Score (heuristic-based)
    schema_health_score = 100
    for col in df.select_dtypes(include=['object']).columns:
        if pd.to_numeric(df[col], errors='coerce').notna().sum() > 0:
             schema_health_score -= 5
    
    for col in df.columns:
        if df[col].nunique() == 1:
            schema_health_score -= 10

    schema_health_score = max(0, schema_health_score)

    # 4. Overall Score (weighted average)
    overall_score = (completeness_score * 0.4) + (consistency_score * 0.4) + (schema_health_score * 0.2)

    return {
        "overall": round(overall_score),
        "completeness": round(completeness_score),
        "consistency": round(consistency_score),
        "schema_health": round(schema_health_score)
    }

def rate_readiness(file_contents: bytes, filename: str):
    """
    Main function to rate the readiness of a dataset from an uploaded file.
    """
    file_extension = filename.split('.')[-1].lower()
    
    readiness_score, rows_analyzed = _read_data_and_calculate_score(file_extension, file_contents, filename)
    
    return {
        "source_file": filename,
        "readiness_score": readiness_score,
        "total_rows_analyzed": rows_analyzed
    }

