from fastapi import APIRouter, UploadFile, File, HTTPException
from app.agents.source import schema_scanner, dedup_agent, readiness_rater
from app.orchestrator import workflow
import pandas as pd


router = APIRouter()
SUPPORTED_FILE_EXTENSIONS = {"csv", "xlsx", "xls","json","sql"}

# Agent endpoints
@router.post("/scan-schema")
async def scan_schema(file: UploadFile = File(...)):
    """
    Accepts a file upload (e.g., CSV), scans its schema, and returns a profile.
    """
    # Ensure the uploaded file is a CSV
    file_extension = file.filename.split('.')[-1].lower()
    if file_extension not in SUPPORTED_FILE_EXTENSIONS:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid file type. Supported types are: {', '.join(SUPPORTED_FILE_EXTENSIONS)}")

    try:
        # Read the file content as bytes
        contents = await file.read()
        # Pass the raw bytes to the agent for processing
        return schema_scanner.scan_schema(contents, file.filename)
    except Exception as e:
        # Handle potential errors during file processing
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")

@router.post("/rate-readiness")
async def rate_readiness_endpoint(file: UploadFile = File(...)):
    """
    Calculates the readiness score for a dataset from an uploaded file.
    Supported file types: CSV, Excel, JSON, Parquet, SQL.
    """
    file_extension = file.filename.split('.')[-1].lower()
    if file_extension not in SUPPORTED_FILE_EXTENSIONS:
        raise HTTPException(status_code=400, detail=f"Unsupported file format for readiness rating: {file_extension}")
    
    contents = await file.read()
    return readiness_rater.rate_readiness(contents, file.filename)

@router.post("/deduplicate")
def deduplicate(items: list[str]):
    """
    Deduplicate list of items
    """
    return dedup_agent.deduplicate(items)

# Orchestrator endpoint
@router.post("/run-workflow")
def run_workflow(dataset: list[dict], items: list[str]):
    """
    Run workflow: schema scan + deduplication
    """
    return workflow(dataset, items)
