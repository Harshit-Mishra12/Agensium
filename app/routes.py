from fastapi import APIRouter, UploadFile, File, HTTPException
from app.agents.source import schema_scanner, dedup_agent
from app.orchestrator import workflow
import pandas as pd


router = APIRouter()

# Agent endpoints
@router.post("/scan-schema")
async def scan_schema(file: UploadFile = File(...)):
    """
    Accepts a file upload (e.g., CSV), scans its schema, and returns a profile.
    """
    # Ensure the uploaded file is a CSV
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")

    try:
        # Read the file content as bytes
        contents = await file.read()
        # Pass the raw bytes to the agent for processing
        return schema_scanner.scan_schema(contents)
    except Exception as e:
        # Handle potential errors during file processing
        raise HTTPException(status_code=500, detail=f"Error processing file: {e}")


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
