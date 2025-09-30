import pandas as pd
import numpy as np
from scipy.stats import ks_2samp, chi2_contingency

class DriftDetector:
    @staticmethod
    def detect_drift(baseline_csv: str, current_csv: str):
        """
        Detect schema and data drift between baseline and current CSVs.
        Returns JSON with drifted fields, drift scores, direction, and p-values.
        """
        baseline_df = pd.read_csv(baseline_csv)
        current_df = pd.read_csv(current_csv)

        drift_report = {}

        # --- Schema Drift ---
        baseline_cols = set(baseline_df.columns)
        current_cols = set(current_df.columns)

        # New columns
        for col in current_cols - baseline_cols:
            drift_report[col] = {"schema_change": f"new column detected: {col}"}

        # Removed columns
        for col in baseline_cols - current_cols:
            drift_report[col] = {"schema_change": f"column missing: {col}"}

        # Data type changes
        for col in baseline_cols & current_cols:
            if baseline_df[col].dtype != current_df[col].dtype:
                drift_report[col] = {"schema_change": f"type change: {baseline_df[col].dtype} → {current_df[col].dtype}"}

        # --- Data Drift ---
        shared_cols = baseline_cols & current_cols
        for col in shared_cols:
            base_col = baseline_df[col].dropna()
            curr_col = current_df[col].dropna()

            if base_col.empty or curr_col.empty:
                continue

            # Numeric drift
            if pd.api.types.is_numeric_dtype(base_col):
                try:
                    ks_stat, p_value = ks_2samp(base_col, curr_col)
                    direction = "increase" if curr_col.mean() > base_col.mean() else "decrease"
                    drift_report[col] = {
                        "drift_score": float(ks_stat),
                        "p_value": float(p_value),
                        "direction": f"{direction} in mean {col}"
                    }
                except Exception as e:
                    drift_report[col] = {"note": f"numeric drift detection failed: {e}"}

            # Categorical drift
            elif pd.api.types.is_object_dtype(base_col):
                try:
                    base_counts = base_col.value_counts()
                    curr_counts = curr_col.value_counts()
                    all_categories = list(set(base_counts.index) | set(curr_counts.index))
                    base_freq = [base_counts.get(cat, 0) for cat in all_categories]
                    curr_freq = [curr_counts.get(cat, 0) for cat in all_categories]

                    chi2, p_value, _, _ = chi2_contingency([base_freq, curr_freq])
                    new_categories = list(set(curr_col.unique()) - set(base_col.unique()))
                    direction = "new categories appeared" if new_categories else "distribution changed"
                    drift_report[col] = {
                        "drift_score": float(chi2),
                        "p_value": float(p_value),
                        "direction": direction
                    }
                except Exception as e:
                    drift_report[col] = {"note": f"categorical drift detection failed: {e}"}

            # Temporal/date drift
            elif pd.api.types.is_datetime64_any_dtype(base_col) or "date" in col.lower():
                try:
                    base_dates = pd.to_datetime(base_col, errors="coerce").dropna()
                    curr_dates = pd.to_datetime(curr_col, errors="coerce").dropna()
                    if not base_dates.empty and not curr_dates.empty:
                        drift_report[col] = {
                            "earliest_baseline": str(base_dates.min().date()),
                            "earliest_current": str(curr_dates.min().date()),
                            "latest_baseline": str(base_dates.max().date()),
                            "latest_current": str(curr_dates.max().date())
                        }
                except Exception as e:
                    drift_report[col] = {"note": f"datetime drift detection failed: {e}"}

        return drift_report
