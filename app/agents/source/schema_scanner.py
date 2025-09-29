import pandas as pd
import io

def scan_schema(file_contents: bytes):
    """
    Scans the schema of a CSV file provided as bytes.

    Args:
        file_contents: The byte content of the CSV file.

    Returns:
        A dictionary containing the schema summary.
    """
    print( "Scanning schema..." )
    try:
        # Read the byte content into a pandas DataFrame
        df = pd.read_csv(io.BytesIO(file_contents))
    except Exception as e:
        return {"error": f"Could not parse CSV file. Details: {e}"}

    # Prepare a list to hold the summary data for each column
    schema_summary = []
    total_rows = len(df)

    # Loop through each column in the DataFrame
    for col in df.columns:
        # 1. Calculate Null Percentage
        null_count = df[col].isnull().sum()
        null_percentage = (null_count / total_rows) * 100 if total_rows > 0 else 0

        # 2. Get Distinct Count
        distinct_count = df[col].nunique()

        # 3. Get Top 3 Values
        top_values = df[col].value_counts().nlargest(3).index.tolist()
        top_values_str = ', '.join(map(str, top_values))

        # 4. Infer Data Type with a fallback
        inferred_type = str(df[col].dtype)
        data_type = "Text" # Default to Text
        if "int" in inferred_type:
            data_type = "Integer"
        elif "float" in inferred_type:
            data_type = "Float"
        elif "datetime" in inferred_type:
            data_type = "Datetime"
        # A simple check to see if an 'object' column could be a date
        elif inferred_type == 'object':
            try:
                pd.to_datetime(df[col], errors='raise', infer_datetime_format=True)
                data_type = 'Date'
            except (ValueError, TypeError):
                data_type = 'Text'

        # Append all the info to our summary list
        schema_summary.append({
            "Field": col,
            "Data Type": data_type,
            "Null %": f"{null_percentage:.1f}%",
            "Distinct Count": distinct_count,
            "Top Values": top_values_str + ('…' if distinct_count > 3 else '')
        })

    return {
        "file_name": "uploaded_file.csv", # In a real app, you'd get this from the UploadFile object
        "total_rows": total_rows,
        "schema_summary": schema_summary
    }
