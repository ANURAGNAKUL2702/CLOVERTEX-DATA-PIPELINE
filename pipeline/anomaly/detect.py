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
	labs_file = latest_file("labs")
	variants_file = latest_file("variants")

	if labs_file is None and variants_file is None:
		print("⚠ No data found for anomaly detection")
		return

	partition = "unpartitioned"
	if labs_file:
		partition = partition_from_path(labs_file)

	anomalies = []

	if labs_file:
		labs = pd.read_parquet(labs_file)
		if "test_name" in labs.columns and "test_value" in labs.columns:
			labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")
			for test, group in labs.groupby("test_name"):
				values = group["test_value"].dropna()
				if values.empty:
					continue
				q1 = values.quantile(0.25)
				q3 = values.quantile(0.75)
				iqr = q3 - q1
				lower = q1 - 1.5 * iqr
				upper = q3 + 1.5 * iqr
				mask = (group["test_value"] < lower) | (group["test_value"] > upper)
				out = group[mask]
				for _, row in out.iterrows():
					anomalies.append({
						"dataset": "labs",
						"patient_id": row.get("patient_id"),
						"field": "test_value",
						"value": row.get("test_value"),
						"reason": f"IQR_outlier:{test}",
					})

	if variants_file:
		variants = pd.read_parquet(variants_file)
		if "allele_frequency" in variants.columns:
			variants["allele_frequency"] = pd.to_numeric(variants["allele_frequency"], errors="coerce")
			mask = variants["allele_frequency"] > 0.5
			out = variants[mask]
			for _, row in out.iterrows():
				anomalies.append({
					"dataset": "variants",
					"patient_id": row.get("patient_id"),
					"field": "allele_frequency",
					"value": row.get("allele_frequency"),
					"reason": "allele_frequency_gt_0.5",
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
