def detect_drift(current_data: list[dict], baseline_data: list[dict]):
    """
    Example: simple drift detection
    Returns number of differing rows
    """
    current_set = {tuple(d.items()) for d in current_data}
    baseline_set = {tuple(d.items()) for d in baseline_data}
    drift_count = len(current_set - baseline_set)
    return {"drift_rows": drift_count}
