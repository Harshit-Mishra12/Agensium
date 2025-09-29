from fastapi import APIRouter
from app.agents.source import schema_scanner, dedup_agent
from app.orchestrator import workflow

router = APIRouter()

# Agent endpoints
@router.post("/scan-schema")
def scan_schema(dataset: list[dict]):
    """
    Scan dataset schema
    """
    return schema_scanner.scan_schema(dataset)

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
