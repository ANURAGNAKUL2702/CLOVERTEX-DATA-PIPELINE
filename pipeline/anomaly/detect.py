import pandas as pd
from pathlib import Path

CONSUMPTION_DIR = Path("datalake/consumption/v1")
PARTITION_PREFIX = "ingest_date="


def latest_partition_dir(dataset_dir: Path) -> Path | None:
	if not dataset_dir.exists():
		return None
	parts = sorted([p for p in dataset_dir.iterdir() if p.is_dir() and p.name.startswith(PARTITION_PREFIX)])
	return parts[-1] if parts else dataset_dir


def latest_file(dataset: str) -> Path | None:
	dataset_dir = CONSUMPTION_DIR / dataset
	part_dir = latest_partition_dir(dataset_dir)
	if part_dir is None:
		return None
	files = list(part_dir.glob("*.parquet"))
	return sorted(files)[-1] if files else None


def partition_from_path(path: Path) -> str:
	parts = path.parts
	part = next((p for p in parts if p.startswith(PARTITION_PREFIX)), None)
	return part or "unpartitioned"


def run_detection():
	unified_file = latest_file("unified", "patients_unified_analytics")
	labs_file = latest_file("labs")
	diag_file = latest_file("diagnoses")
	meds_file = latest_file("medications")
	variants_file = latest_file("variants")

	if unified_file is None and labs_file is None and variants_file is None:
		print("⚠ No data found for anomaly detection")
		return

	partition = "unpartitioned"
	if unified_file:
		partition = partition_from_path(unified_file)
	elif labs_file:
		partition = partition_from_path(labs_file)

	anomalies = []

	patient_ids = set()
	if unified_file:
		unified = pd.read_parquet(unified_file)
		if "patient_id" in unified.columns:
			patient_ids = set(unified["patient_id"].dropna().astype(str).unique())

		# Impossible age
		dob_col = next((c for c in ["date_of_birth", "birthdate", "dob"] if c in unified.columns), None)
		if dob_col:
			dob = pd.to_datetime(unified[dob_col], errors="coerce")
			today = pd.Timestamp.now("UTC").normalize().tz_localize(None)
			age = ((today - dob.dt.tz_localize(None)).dt.days / 365.25).astype("float")
			mask = age.lt(0) | age.gt(120)
			for _, row in unified[mask].iterrows():
				anomalies.append({
					"dataset": "unified",
					"patient_id": row.get("patient_id"),
					"field": dob_col,
					"value": row.get(dob_col),
					"reason": "impossible_age",
					"threshold_low": 0,
					"threshold_high": 120,
					"is_anomaly": True,
				})

		# Missing critical fields
		critical = ["patient_id"]
		if dob_col:
			critical.append(dob_col)
		missing = unified[critical].isna().any(axis=1)
		for _, row in unified[missing].iterrows():
			anomalies.append({
				"dataset": "unified",
				"patient_id": row.get("patient_id"),
				"field": "|".join(critical),
				"value": None,
				"reason": "missing_critical_fields",
				"threshold_low": None,
				"threshold_high": None,
				"is_anomaly": True,
			})

	if labs_file:
		labs = pd.read_parquet(labs_file)
		if "test_name" in labs.columns and "test_value" in labs.columns:
			labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")
			for test, group in labs.groupby("test_name"):
				values = group["test_value"].dropna()
				if values.empty:
					continue
				median = values.median()
				q1 = values.quantile(0.25)
				q3 = values.quantile(0.75)
				iqr = q3 - q1
				lower = median - 3 * iqr
				upper = median + 3 * iqr
				mask = (group["test_value"] < lower) | (group["test_value"] > upper)
				out = group[mask]
				for _, row in out.iterrows():
					anomalies.append({
						"dataset": "labs",
						"patient_id": row.get("patient_id"),
						"field": "test_value",
						"value": row.get("test_value"),
						"reason": f"extreme_lab_value:{test}",
						"threshold_low": float(lower),
						"threshold_high": float(upper),
						"is_anomaly": True,
					})

		if {"patient_id", "test_name", "collection_date"}.issubset(set(labs.columns)):
			dup_mask = labs.duplicated(subset=["patient_id", "test_name", "collection_date"], keep=False)
			for _, row in labs[dup_mask].iterrows():
				anomalies.append({
					"dataset": "labs",
					"patient_id": row.get("patient_id"),
					"field": "patient_id|test_name|collection_date",
					"value": None,
					"reason": "duplicate_visit",
					"threshold_low": None,
					"threshold_high": None,
					"is_anomaly": True,
				})

	# Orphan records across datasets
	for dataset, file in [
		("labs", labs_file),
		("diagnoses", diag_file),
		("medications", meds_file),
		("variants", variants_file),
	]:
		if file is None or not patient_ids:
			continue
		df = pd.read_parquet(file)
		if "patient_id" not in df.columns:
			continue
		orphans = df[~df["patient_id"].astype("string").isin(patient_ids)]
		for _, row in orphans.iterrows():
			anomalies.append({
				"dataset": dataset,
				"patient_id": row.get("patient_id"),
				"field": "patient_id",
				"value": row.get("patient_id"),
				"reason": "orphan_record",
				"threshold_low": None,
				"threshold_high": None,
				"is_anomaly": True,
			})

	output_dir = CONSUMPTION_DIR / "analytics"
	if partition != "unpartitioned":
		output_dir = output_dir / partition
	output_dir.mkdir(parents=True, exist_ok=True)

	out_path = output_dir / "anomaly_detection.parquet"
	tmp_path = output_dir / "anomaly_detection.tmp"
	pd.DataFrame(anomalies).to_parquet(tmp_path, index=False)
	tmp_path.replace(out_path)

	print(f"✅ Anomaly detection saved → {out_path}")


if __name__ == "__main__":
	run_detection()
