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
CLEANING_FILE_METRICS_PATH = LOG_QUALITY_DIR / "cleaning_file_metrics.json"
PARTITION_PREFIX = "ingest_date="
RAW_DIR = Path("datalake/raw")


def latest_ingestion_log() -> Path | None:
	if not LOG_INGESTION_DIR.exists():
		return None
	logs = sorted(LOG_INGESTION_DIR.glob("ingestion_log_*.json"))
	return logs[-1] if logs else None


def load_json(path: Path) -> dict | list:
	with open(path, "r", encoding="utf-8") as f:
		return json.load(f)


def stable_generated_timestamp() -> str:
	files = [f for f in RAW_DIR.rglob("*") if f.is_file()]
	if not files:
		return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

	latest = max(f.stat().st_mtime for f in files)
	return datetime.fromtimestamp(latest, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


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


def orphan_records_by_source_file(files: list[Path]) -> dict:
	patients_file = next((f for f in files if f.name == "patients_unified_analytics.parquet"), None)
	if patients_file is None:
		return {}

	patients = pd.read_parquet(patients_file)
	if "patient_id" not in patients.columns:
		return {}

	patient_ids = set(patients["patient_id"].dropna().astype(str).unique())

	orphans: dict[str, int] = {}
	for file in files:
		parts = file.parts
		dataset = parts[parts.index("v1") + 1] if "v1" in parts else file.parent.name
		if dataset not in {"labs", "diagnoses", "medications", "variants"}:
			continue

		df = pd.read_parquet(file)
		if "patient_id" not in df.columns or "_source_file" not in df.columns:
			continue

		for source_file, group in df.groupby(df["_source_file"].astype("string"), dropna=False):
			key = str(source_file) if pd.notna(source_file) else "unknown"
			ids = set(group["patient_id"].dropna().astype(str).unique())
			missing = ids - patient_ids
			orphans[key] = orphans.get(key, 0) + len(missing)

	return orphans


def run_validation():
	LOG_QUALITY_DIR.mkdir(parents=True, exist_ok=True)

	report = {
		"generated": stable_generated_timestamp(),
		"ingestion": {},
		"cleaning": {},
		"quality_by_file": [],
		"quality_by_source_file": [],
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

	cleaning_file_metrics = []
	if CLEANING_FILE_METRICS_PATH.exists():
		cleaning_file_metrics = load_json(CLEANING_FILE_METRICS_PATH)

	orphans_by_source = orphan_records_by_source_file(files)
	for entry in cleaning_file_metrics:
		source_file = entry.get("source_file")
		report["quality_by_source_file"].append(
			{
				"source_file": source_file,
				"dataset": entry.get("dataset"),
				"nulls_handled": int(entry.get("nulls_handled", 0)),
				"duplicates_removed": int(entry.get("duplicates_removed", 0)),
				"orphan_records_found": int(orphans_by_source.get(source_file, 0)),
				"schema_mismatches_fixed": int(entry.get("schema_mismatches_fixed", 0)),
			}
		)

	with open(REPORT_PATH, "w", encoding="utf-8") as f:
		json.dump(report, f, indent=2)

	print(f"✅ Data quality report saved → {REPORT_PATH}")


if __name__ == "__main__":
	run_validation()
