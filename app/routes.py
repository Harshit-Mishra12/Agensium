from fastapi import APIRouter, UploadFile, File, HTTPException
from app.agents.source import schema_scanner, dedup_agent, field_profiler, drift_detector
from app.orchestrator import workflow
import pandas as pd
from tempfile import NamedTemporaryFile
import shutil
import io

router = APIRouter()

# --- Schema scanner endpoint ---
@router.post("/scan-schema")
async def scan_schema(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    try:
        contents = await file.read()
        return schema_scanner.scan_schema(contents)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")


# --- Field profiler endpoint (CSV or SQL) ---
@router.post("/field-profiler")
async def profile_dataset(file: UploadFile = File(...)):
    try:
        contents = await file.read()

        if file.filename.endswith(".csv"):
            return {"tables": {"csv_file": field_profiler.profile_csv(contents)}}

        elif file.filename.endswith(".sql"):
            return field_profiler.profile_sql(contents)

        else:
            raise HTTPException(status_code=400, detail="Unsupported file type. Upload .csv or .sql")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")


# --- Deduplicate endpoint ---
@router.post("/deduplicate")
def deduplicate(items: list[str]):
    return dedup_agent.deduplicate(items)


# --- Orchestrator endpoint ---
@router.post("/run-workflow")
def run_workflow(dataset: list[dict], items: list[str]):
    return workflow(dataset, items)


# --- Drift Detector endpoint ---
@router.post("/detect-drift")
async def detect_drift(
    baseline_file: UploadFile = File(...),
    current_file: UploadFile = File(...)
):
    """
    Detect drift between baseline CSV and current CSV.
    Returns JSON report with schema and data drift.
    """
    if not baseline_file.filename.endswith(".csv") or not current_file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Both files must be CSVs.")

    try:
        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_base:
            shutil.copyfileobj(baseline_file.file, tmp_base)
            baseline_path = tmp_base.name

        with NamedTemporaryFile(delete=False, suffix=".csv") as tmp_curr:
            shutil.copyfileobj(current_file.file, tmp_curr)
            current_path = tmp_curr.name

        report = drift_detector.DriftDetector.detect_drift(baseline_path, current_path)
        return report

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error detecting drift: {e}")
