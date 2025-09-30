import pandas as pd
import numpy as np
import sqlite3
import io
from scipy.stats import entropy

def _profile_dataframe(df: pd.DataFrame, baseline_schema: dict | None = None):
    """
    Profile a single dataframe (field stats, anomalies, drift) with 2-decimal rounding.
    """
    results = {"field_statistics": {}, "anomalies": {}}

    # --- Field-level stats ---
    for col in df.columns:
        col_data = df[col].dropna()

        if col_data.empty:
            continue

        if pd.api.types.is_numeric_dtype(col_data):
            stats = {
                "min": round(float(col_data.min()), 2),
                "max": round(float(col_data.max()), 2),
                "mean": round(float(col_data.mean()), 2),
                "std_dev": round(float(col_data.std()), 2),
                "entropy": round(float(entropy(pd.value_counts(col_data, normalize=True), base=2)), 2),
            }
        elif pd.api.types.is_datetime64_any_dtype(col_data) or "date" in col.lower():
            try:
                col_data = pd.to_datetime(col_data, errors="coerce").dropna()
                stats = {
                    "min": str(col_data.min().date()) if not col_data.empty else None,
                    "max": str(col_data.max().date()) if not col_data.empty else None,
                    "temporal_spread_days": int((col_data.max() - col_data.min()).days)
                    if not col_data.empty else None,
                }
            except Exception:
                stats = {"note": "Invalid datetime format"}
        else:
            stats = {
                "unique_values": int(col_data.nunique()),
                "entropy": round(float(entropy(pd.value_counts(col_data, normalize=True), base=2)), 2),
            }

        results["field_statistics"][col] = stats

    # --- Anomaly detection ---
    anomalies = {}

    # Missing values %
    missing = df.isnull().mean()
    anomalies["missing_values"] = {col: round(p * 100, 2) for col, p in missing.items() if p > 0}

    # Schema drift check
    if baseline_schema:
        drift = {}
        for col, values in baseline_schema.items():
            if col in df.columns and df[col].dtype == "object":
                new_values = set(df[col].dropna().unique())
                baseline_values = set(values)
                new_categories = new_values - baseline_values
                if new_categories:
                    drift[col] = {"new_categories": list(new_categories)}
        if drift:
            anomalies["schema_drift"] = drift

    # Outliers (IQR method)
    outliers = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        q1 = df[col].quantile(0.25)
        q3 = df[col].quantile(0.75)
        iqr = q3 - q1
        high_threshold = q3 + 1.5 * iqr
        low_threshold = q1 - 1.5 * iqr
        outlier_values = df[(df[col] > high_threshold) | (df[col] < low_threshold)][col].tolist()
        if outlier_values:
            # Round outlier values to 2 decimals
            outliers[col] = [round(float(x), 2) for x in outlier_values]
    if outliers:
        anomalies["outliers"] = outliers

    results["anomalies"] = anomalies
    return results

# --- File type handlers ---
def profile_csv(contents: bytes):
    df = pd.read_csv(io.BytesIO(contents))
    return _profile_dataframe(df)

def profile_excel(contents: bytes):
    df = pd.read_excel(io.BytesIO(contents))
    return _profile_dataframe(df)

def profile_json(contents: bytes):
    df = pd.read_json(io.BytesIO(contents))
    return _profile_dataframe(df)

def profile_sql(contents: bytes):
    sql_text = contents.decode("utf-8")
    conn = sqlite3.connect(":memory:")

    try:
        cursor = conn.cursor()
        cursor.executescript(sql_text)

        tables_df = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)
        results = {"tables": {}}

        for table in tables_df["name"].tolist():
            df = pd.read_sql(f"SELECT * FROM {table};", conn)
            results["tables"][table] = _profile_dataframe(df)

        return results
    finally:
        conn.close()
