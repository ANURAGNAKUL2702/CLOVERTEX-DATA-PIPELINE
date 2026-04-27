import json
from pathlib import Path
import pandas as pd
from datetime import datetime, timezone

# -------------------------------
# CONFIG
# -------------------------------
CONSUMPTION_DIR = Path("datalake/consumption/v1")
LOG_INGESTION_DIR = Path("logs/ingestion")
LOG_QUALITY_DIR = Path("logs/quality")
REPORT_PATH = LOG_QUALITY_DIR / "data_quality_report.json"
PARTITION_PREFIX = "ingest_date="


def latest_ingestion_log() -> Path | None:
	if not LOG_INGESTION_DIR.exists():
		return None
	logs = sorted(LOG_INGESTION_DIR.glob("ingestion_log_*.json"))
	return logs[-1] if logs else None


def load_json(path: Path) -> dict | list:
	with open(path, "r", encoding="utf-8") as f:
		return json.load(f)


def iter_consumption_files():
	files = list(CONSUMPTION_DIR.glob(f"**/{PARTITION_PREFIX}*/*.parquet"))
	if not files:
		files = list(CONSUMPTION_DIR.glob("*/*.parquet"))
	return files


def file_quality_metrics(files: list[Path]) -> list[dict]:
	metrics = []
	for file in files:
		parts = file.parts
		dataset = parts[parts.index("v1") + 1] if "v1" in parts else file.parent.name
		df = pd.read_parquet(file)
		metrics.append({
			"file": file.as_posix(),
			"dataset": dataset,
			"row_count": int(len(df)),
			"nulls_by_column": {k: int(v) for k, v in df.isna().sum().to_dict().items()},
			"duplicate_rows": int(df.duplicated().sum()),
		})
	return metrics


def schema_mismatches(files: list[Path]) -> dict:
	schemas: dict[str, list[set[str]]] = {}
	for file in files:
		parts = file.parts
		dataset = parts[parts.index("v1") + 1] if "v1" in parts else file.parent.name
		cols = set(pd.read_parquet(file).columns)
		schemas.setdefault(dataset, []).append(cols)

	mismatches = {}
	for dataset, col_sets in schemas.items():
		base = col_sets[0]
		diffs = []
		for cols in col_sets[1:]:
			if cols != base:
				diffs.append({
					"only_in_base": sorted(list(base - cols)),
					"only_in_other": sorted(list(cols - base)),
				})
		if diffs:
			mismatches[dataset] = diffs
	return mismatches


def orphan_records(files: list[Path]) -> dict:
	patients_file = next((f for f in files if f.name == "patients_unified_analytics.parquet"), None)
	if patients_file is None:
		return {}

	patients = pd.read_parquet(patients_file)
	if "patient_id" not in patients.columns:
		return {}

	patient_ids = set(patients["patient_id"].dropna().astype(str).unique())

	orphans = {}
	for file in files:
		parts = file.parts
		dataset = parts[parts.index("v1") + 1] if "v1" in parts else file.parent.name
		if dataset not in {"labs", "diagnoses", "medications", "variants"}:
			continue

		df = pd.read_parquet(file)
		if "patient_id" not in df.columns:
			continue

		ids = set(df["patient_id"].dropna().astype(str).unique())
		missing = ids - patient_ids
		orphans[dataset] = len(missing)

	return orphans


def run_validation():
	LOG_QUALITY_DIR.mkdir(parents=True, exist_ok=True)

	report = {
		"generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
		"ingestion": {},
		"cleaning": {},
		"quality_by_file": [],
		"orphan_records": {},
		"schema_mismatches": {},
	}

	latest_log = latest_ingestion_log()
	if latest_log:
		report["ingestion"] = load_json(latest_log)

	cleaning_metrics_path = LOG_QUALITY_DIR / "cleaning_metrics.json"
	if cleaning_metrics_path.exists():
		report["cleaning"] = load_json(cleaning_metrics_path)

	files = iter_consumption_files()
	report["quality_by_file"] = file_quality_metrics(files)
	report["orphan_records"] = orphan_records(files)
	report["schema_mismatches"] = schema_mismatches(files)

	with open(REPORT_PATH, "w", encoding="utf-8") as f:
		json.dump(report, f, indent=2)

	print(f"✅ Data quality report saved → {REPORT_PATH}")


if __name__ == "__main__":
	run_validation()
