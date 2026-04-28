import pandas as pd
from pathlib import Path
import json
import re

# -------------------------------
# CONFIG
# -------------------------------
INPUT_DIR = Path("data/raw_input")
RAW_DIR = Path("datalake/raw")
LOG_DIR = Path("logs/ingestion")

SUPPORTED_EXT = [".csv", ".xlsx", ".xls", ".parquet", ".json"]


# -------------------------------
# SAFE FILENAME
# -------------------------------
def safe_filename(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_.-]+", "_", name)
    return name.replace(".", "_").strip("_")


# -------------------------------
# LOAD JSON (ROBUST)
# -------------------------------
def load_json(path: Path) -> pd.DataFrame:
    try:
        return pd.read_json(path, lines=True)
    except ValueError:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)

        if isinstance(data, list) or isinstance(data, dict):
            return pd.json_normalize(data)
        else:
            raise ValueError("Unsupported JSON structure")


# -------------------------------
# LOAD FILE BASED ON TYPE
# -------------------------------
def load_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()

    if ext == ".csv":
        return pd.read_csv(path)

    elif ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)

    elif ext == ".parquet":
        return pd.read_parquet(path)

    elif ext == ".json":
        return load_json(path)

    else:
        raise ValueError(f"Unsupported file: {path}")


# -------------------------------
# NORMALIZE COLUMN NAMES
# -------------------------------
def normalize_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    original_cols = list(df.columns)
    df.columns = df.columns.astype(str)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    changed = original_cols != list(df.columns)
    return df, changed


# -------------------------------
# STANDARDIZE PATIENT KEY
# -------------------------------
def standardize_patient_id(df: pd.DataFrame) -> tuple[pd.DataFrame, bool]:
    aliases = ["patient_ref", "patientid", "patient_id", "patient"]
    changed = False

    for col in df.columns:
        clean = col.lower().replace("-", "_")

        if clean in aliases:
            if col != "patient_id":
                df = df.rename(columns={col: "patient_id"})
                changed = True
            break

    return df, changed


# -------------------------------
# STRICT DATASET BUCKET
# -------------------------------
def get_dataset_bucket(filename: str) -> str:
    name = filename.lower()

    if "note" in name:
        return "notes"
    elif "diagnos" in name or "icd" in name:
        return "diagnoses"
    elif "variant" in name or "genomic" in name:
        return "variants"
    elif "lab" in name or "test" in name:
        return "labs"
    elif "patient" in name:
        return "patients"
    elif "medication" in name:
        return "medications"
    else:
        raise ValueError(f"Unknown dataset for file: {filename}")


# -------------------------------
# SAVE (PARTITION + ATOMIC WRITE)
# -------------------------------
def save_to_raw(df: pd.DataFrame, dataset: str, filename: str, ingest_date):
    output_path = RAW_DIR / dataset / f"ingest_date={ingest_date}"
    output_path.mkdir(parents=True, exist_ok=True)

    safe_name = safe_filename(filename)
    final_file = output_path / f"{safe_name}.parquet"
    temp_file = output_path / f"{safe_name}.tmp"

    if final_file.exists():
        return final_file, True

    df.to_parquet(temp_file, index=False)
    temp_file.replace(final_file)

    return final_file, False


# -------------------------------
# MAIN INGESTION
# -------------------------------
def run_ingestion():
    print("🚀 Starting Ingestion...\n")

    # Input directory check
    if not INPUT_DIR.exists():
        raise FileNotFoundError(f"Input dir not found: {INPUT_DIR}")

    files = [f for f in INPUT_DIR.iterdir() if f.suffix.lower() in SUPPORTED_EXT]

    if not files:
        print("❌ No files found in data/raw_input/")
        return

    success, failed, skipped, total_rows = 0, 0, 0, 0
    dataset_metrics = {}

    for file in files:
        try:
            print(f"[INFO] Processing: {file.name}")

            # -------------------------------
            # LOAD
            # -------------------------------
            df = load_file(file)

            if df.empty:
                raise ValueError("Empty file")

            # -------------------------------
            # NORMALIZE
            # -------------------------------
            df, cols_changed = normalize_columns(df)
            df, id_changed = standardize_patient_id(df)

            # -------------------------------
            # DROP EMPTY COLUMNS
            # -------------------------------
            df = df.dropna(axis=1, how="all")

            # -------------------------------
            # DATASET BUCKET
            # -------------------------------
            dataset = get_dataset_bucket(file.name)

            # -------------------------------
            # CONSISTENT DATE (UTC) FOR IDEMPOTENCY
            # -------------------------------
            ingestion_ts = pd.Timestamp.now("UTC")
            ingest_date = pd.Timestamp.now("UTC").date()

            # -------------------------------
            # METADATA
            # -------------------------------
            df["_source_file"] = file.name
            df["_ingestion_time"] = ingestion_ts
            df["_dataset"] = dataset
            df["_batch_id"] = ingestion_ts.strftime("%Y%m%d%H%M%S")

            # -------------------------------
            # SAVE
            # -------------------------------
            output_file, already_exists = save_to_raw(df, dataset, file.name, ingest_date)

            # -------------------------------
            # METRICS (CONTROLLED LOGGING)
            # -------------------------------
            rows, cols = df.shape
            total_rows += rows

            null_counts = df.isnull().sum()
            null_cols = null_counts[null_counts > 0]
            dup_count = int(df.duplicated().sum())

            print(f"[INFO] Rows: {rows}, Cols: {cols}")
            if already_exists:
                print(f"[INFO] Skipped (already exists) → {output_file}")
                skipped += 1
            else:
                print(f"[INFO] Saved → {output_file}")

            if not null_cols.empty:
                print(f"[WARN] Null columns: {null_cols.to_dict()}")
            if dup_count > 0:
                print(f"[WARN] Duplicate rows: {dup_count}")

            print()

            if not already_exists:
                success += 1

            metrics = dataset_metrics.setdefault(
                dataset,
                {
                    "dataset": dataset,
                    "rows_in": 0,
                    "rows_out": 0,
                    "issues_found": {
                        "null_columns": {},
                        "duplicate_rows": 0,
                        "schema_fixes": [],
                    },
                },
            )

            metrics["rows_in"] += rows
            metrics["rows_out"] += rows

            if not null_cols.empty:
                for k, v in null_cols.to_dict().items():
                    metrics["issues_found"]["null_columns"][k] = (
                        metrics["issues_found"]["null_columns"].get(k, 0) + int(v)
                    )

            metrics["issues_found"]["duplicate_rows"] += dup_count

            if cols_changed and "normalized_columns" not in metrics["issues_found"]["schema_fixes"]:
                metrics["issues_found"]["schema_fixes"].append("normalized_columns")

            if id_changed and "standardized_patient_id" not in metrics["issues_found"]["schema_fixes"]:
                metrics["issues_found"]["schema_fixes"].append("standardized_patient_id")

        except Exception as e:
            print(f"[ERROR] Failed: {file.name} → {e}\n")
            failed += 1

    # -------------------------------
    # SUMMARY
    # -------------------------------
    print("📊 Ingestion Summary")
    print(f"[INFO] Success     : {success}")
    print(f"[INFO] Failed      : {failed}")
    print(f"[INFO] Skipped     : {skipped}")
    print(f"[INFO] Total Rows  : {total_rows}")

    if dataset_metrics:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        batch_id = pd.Timestamp.now("UTC").strftime("%Y%m%d%H%M%S")
        log_path = LOG_DIR / f"ingestion_log_{batch_id}.json"
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(list(dataset_metrics.values()), f, indent=2)
        print(f"[INFO] Log saved   : {log_path}")
        print("[INFO] Metrics JSON:")
        print(json.dumps(list(dataset_metrics.values()), indent=2))


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run_ingestion()
