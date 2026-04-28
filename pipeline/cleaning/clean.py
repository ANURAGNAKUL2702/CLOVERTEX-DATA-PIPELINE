import hashlib
import json
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
import re

import pandas as pd
  
# -------------------------------
# CONFIG
# -------------------------------
RAW_DIR = Path("datalake/raw")
REFINED_DIR = Path("datalake/refined/v1")
LOG_QUALITY_DIR = Path("logs/quality")
METRICS_PATH = LOG_QUALITY_DIR / "cleaning_metrics.json"
FILE_METRICS_PATH = LOG_QUALITY_DIR / "cleaning_file_metrics.json"
PARTITION_PREFIX = "ingest_date="
SUPPORTED_EXT = [".csv", ".xlsx", ".xls", ".parquet", ".json"]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_filename(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_.-]+", "_", name)
    return name.replace(".", "_").strip("_")


def ingestion_timestamp_from_mtime(path: Path) -> str:
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return mtime.strftime("%Y-%m-%dT%H:%M:%SZ")


def batch_id_from_hash(path: Path) -> str:
    return sha256_file(path)[0:12]


def load_json_text(text: str) -> pd.DataFrame:
    try:
        return pd.read_json(StringIO(text), lines=True)
    except ValueError:
        data = json.loads(text)
        if isinstance(data, list) or isinstance(data, dict):
            return pd.json_normalize(data)
        raise ValueError("Unsupported JSON structure")


def load_raw_file(path: Path) -> tuple[pd.DataFrame, int]:
    ext = path.suffix.lower()
    encoding_fixed = 0

    if ext == ".parquet":
        return pd.read_parquet(path), encoding_fixed

    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path), encoding_fixed

    if ext in {".csv", ".json"}:
        try:
            raw_text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            raw_text = path.read_text(encoding="latin-1")
            encoding_fixed = 1

        if ext == ".csv":
            return pd.read_csv(StringIO(raw_text)), encoding_fixed

        return load_json_text(raw_text), encoding_fixed

    raise ValueError(f"Unsupported file: {path}")


# -------------------------------
# STANDARDIZE PATIENT ID
# -------------------------------
def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = df.columns.astype(str)
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_", regex=False)
        .str.replace("-", "_", regex=False)
    )
    return df


def standardize_patient_id(df: pd.DataFrame) -> pd.DataFrame:
    aliases = {"patient_ref", "patientid", "patient_id", "patient"}
    for col in df.columns:
        clean = col.lower().replace("-", "_")
        if clean in aliases and col != "patient_id":
            df = df.rename(columns={col: "patient_id"})
            break
    return df


# -------------------------------
# FLATTEN JSON COLUMNS
# -------------------------------
def flatten_json_columns(df: pd.DataFrame) -> pd.DataFrame:
    for col in list(df.columns):
        if df[col].dtype == "object":
            mask = df[col].apply(lambda x: isinstance(x, dict))

            if mask.any():
                expanded = pd.json_normalize(df.loc[mask, col])
                expanded.index = df.loc[mask].index
                expanded = expanded.add_prefix(f"{col}_")
                expanded = expanded.reindex(df.index)
                df = pd.concat([df.drop(columns=[col]), expanded], axis=1)

    return df


# -------------------------------
# CONVERT DATES
# -------------------------------
def convert_dates(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if "date" in col or col.endswith("_dt"):
            df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


# -------------------------------
# CLEAN ONE DATAFRAME
# -------------------------------
def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    # 0. Normalize columns for consistency across sources
    df = normalize_columns(df)

    # 1. Standardize patient_id
    df = standardize_patient_id(df)

    # 2. Flatten JSON
    df = flatten_json_columns(df)

    # 3. Drop fully empty columns
    df = df.dropna(axis=1, how="all")

    # 4. Standardize missing values
    df = df.replace(["", "NA", "N/A", "null", "None"], pd.NA, regex=False)

    # 5. Strip whitespace
    for col in df.select_dtypes(include="object"):
        if not df[col].apply(lambda x: isinstance(x, dict)).any():
            df[col] = df[col].astype("string").str.strip()

    # 6. Normalize categorical columns
    for col in df.columns:
        if any(key in col for key in ["category", "status", "severity"]):
            df[col] = df[col].str.lower()

    # 7. Convert date columns
    df = convert_dates(df)

    return df


def align_schema(df: pd.DataFrame, dataset: str, schema_map: dict[str, set[str]]) -> tuple[pd.DataFrame, int]:
    expected = schema_map.get(dataset, set())
    if not expected:
        return df, 0

    missing = [c for c in sorted(expected) if c not in df.columns]
    if not missing:
        return df, 0

    for col in missing:
        df[col] = pd.NA

    return df, len(missing)


def fill_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in df.columns:
        if df[col].isna().sum() == 0:
            continue

        if pd.api.types.is_numeric_dtype(df[col]):
            median = pd.to_numeric(df[col], errors="coerce").median()
            if pd.notna(median):
                df[col] = df[col].fillna(median)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            median = pd.to_datetime(df[col], errors="coerce").dropna().median()
            if pd.notna(median):
                df[col] = df[col].fillna(median)
        elif pd.api.types.is_bool_dtype(df[col]):
            df[col] = df[col].fillna(False)
        else:
            df[col] = df[col].astype("string").fillna("unknown")

    return df


def safe_partition_value(value: str) -> str:
    clean = str(value).strip().lower()
    clean = clean.replace(" ", "_").replace("/", "_").replace("-", "_")
    return "unknown" if clean == "" else clean


def pick_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    return next((c for c in candidates if c in df.columns), None)


def build_patient_dedup_key(df: pd.DataFrame) -> pd.Series | None:
    first = pick_column(df, ["first_name", "firstname", "given_name"])
    last = pick_column(df, ["last_name", "lastname", "surname", "family_name"])
    dob = pick_column(df, ["date_of_birth", "birthdate", "dob"])
    sex = pick_column(df, ["sex", "gender"])

    if not first or not last or not dob:
        if "patient_id" in df.columns:
            return df["patient_id"].astype("string")
        return None

    dob_vals = pd.to_datetime(df[dob], errors="coerce").dt.strftime("%Y-%m-%d")
    parts = [
        df[first].astype("string").str.lower().str.strip().fillna(""),
        df[last].astype("string").str.lower().str.strip().fillna(""),
        dob_vals.astype("string").fillna(""),
    ]
    if sex:
        parts.append(df[sex].astype("string").str.lower().str.strip().fillna(""))

    key = parts[0]
    for part in parts[1:]:
        key = key + "|" + part

    return key


# -------------------------------
# SAVE CLEANED DATA
# -------------------------------
def save_clean(df: pd.DataFrame, dataset: str, filename: str):
    output_path = REFINED_DIR / dataset
    output_path.mkdir(parents=True, exist_ok=True)

    final_file = output_path / filename
    temp_file = output_path / f"{filename}.tmp"
    df.to_parquet(temp_file, index=False)
    temp_file.replace(final_file)


def output_parquet_name(path: Path) -> str:
    return f"{safe_filename(path.stem)}.parquet"


def dataset_from_path(path: Path) -> str:
    parts = path.parts
    return parts[parts.index("raw") + 1] if "raw" in parts else path.parent.name


def collect_schema_map(raw_files: list[Path]) -> dict[str, set[str]]:
    schema_map: dict[str, set[str]] = {}
    for file in raw_files:
        dataset = dataset_from_path(file)
        df, _ = load_raw_file(file)
        df = clean_dataframe(df)
        schema_map.setdefault(dataset, set()).update(set(df.columns))
    return schema_map


def add_metadata(df: pd.DataFrame, source_file: Path, dataset: str) -> pd.DataFrame:
    df = df.copy()
    df["_source_file"] = source_file.name
    df["_ingestion_time"] = ingestion_timestamp_from_mtime(source_file)
    df["_dataset"] = dataset
    df["_batch_id"] = batch_id_from_hash(source_file)
    return df


def iter_raw_files():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Raw dir not found: {RAW_DIR}")

    files = [
        f for f in RAW_DIR.rglob("*")
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXT
    ]

    return files


# -------------------------------
# MAIN CLEANING PIPELINE
# -------------------------------
def run_cleaning():
    print("🧹 Starting Cleaning...\n")

    raw_files = iter_raw_files()
    schema_map = collect_schema_map(raw_files)
    success, failed, total_rows = 0, 0, 0
    metrics: dict[str, dict[str, int]] = {}
    file_metrics: list[dict[str, int | str]] = []
    dataset_totals: dict[str, dict[str, int]] = {}
    patient_frames: dict[str, list[pd.DataFrame]] = {}

    for file in raw_files:
        try:
            # raw/<dataset>/ingest_date=YYYY-MM-DD/<file>
            parts = file.parts
            dataset_name = dataset_from_path(file)
            ingest_partition = next((p for p in parts if p.startswith(PARTITION_PREFIX)), None)

            print(f"📂 Cleaning: {dataset_name}/{file.name}")

            df, encoding_fixed = load_raw_file(file)

            if df.empty:
                raise ValueError("Empty file")

            before_rows, before_cols = df.shape

            # Clean
            df = clean_dataframe(df)
            df, schema_fixed = align_schema(df, dataset_name, schema_map)
            before_missing = int(df.isna().sum().sum())
            df = fill_missing_values(df)
            after_missing = int(df.isna().sum().sum())

            df = add_metadata(df, file, dataset_name)

            df = df.drop_duplicates()
            after_rows, after_cols = df.shape
            total_rows += after_rows

            nulls_handled = max(before_missing - after_missing, 0)
            duplicates_removed = max(before_rows - after_rows, 0)

            metrics_entry = metrics.setdefault(
                dataset_name,
                {"nulls_handled": 0, "duplicates_removed": 0, "encoding_fixed": 0},
            )
            metrics_entry["nulls_handled"] += int(nulls_handled)
            metrics_entry["duplicates_removed"] += int(duplicates_removed)
            metrics_entry["encoding_fixed"] += int(encoding_fixed)

            dataset_entry = dataset_totals.setdefault(
                dataset_name,
                {
                    "rows_in": 0,
                    "rows_out": 0,
                    "nulls_handled": 0,
                    "duplicates_removed": 0,
                    "encoding_fixed": 0,
                },
            )
            dataset_entry["rows_in"] += int(before_rows)
            dataset_entry["rows_out"] += int(after_rows)
            dataset_entry["nulls_handled"] += int(nulls_handled)
            dataset_entry["duplicates_removed"] += int(duplicates_removed)
            dataset_entry["encoding_fixed"] += int(encoding_fixed)

            # Save with same partitioning (plus lab-specific partitioning)
            output_dir = REFINED_DIR / dataset_name
            if ingest_partition:
                output_dir = output_dir / ingest_partition

            if dataset_name == "labs" and "test_name" in df.columns:
                for test_name, group in df.groupby(df["test_name"].astype("string"), dropna=False):
                    test_partition = f"test_name={safe_partition_value(test_name)}"
                    partition_dir = REFINED_DIR / dataset_name / test_partition
                    if ingest_partition:
                        partition_dir = partition_dir / ingest_partition
                    partition_dir.mkdir(parents=True, exist_ok=True)
                    save_clean(group, str(partition_dir.relative_to(REFINED_DIR)), output_parquet_name(file))
                saved_message = f"{REFINED_DIR / dataset_name} (partitioned by test_name)"
            else:
                output_dir.mkdir(parents=True, exist_ok=True)
                save_clean(df, str(output_dir.relative_to(REFINED_DIR)), output_parquet_name(file))
                saved_message = str(output_dir)

            stats = {
                "dataset": dataset_name,
                "rows_before": int(before_rows),
                "rows_after": int(after_rows),
                "nulls_handled": int(nulls_handled),
                "duplicates_removed": int(duplicates_removed),
            }

            file_metrics.append(
                {
                    "source_file": file.name,
                    "dataset": dataset_name,
                    "rows_in": int(before_rows),
                    "rows_out": int(after_rows),
                    "nulls_handled": int(nulls_handled),
                    "duplicates_removed": int(duplicates_removed),
                    "encoding_fixed": int(encoding_fixed),
                    "schema_mismatches_fixed": int(schema_fixed),
                }
            )

            print(f"   Rows: {before_rows} → {after_rows}, Cols: {before_cols} → {after_cols}")
            print(f"   Stats: {json.dumps(stats)}")
            print(f"   Saved → {saved_message}\n")

            if dataset_name == "patients":
                key = ingest_partition or "unpartitioned"
                patient_frames.setdefault(key, []).append(df)

            success += 1

        except Exception as e:
            print(f"❌ Error: {file.name} → {e}\n")
            failed += 1

    # -------------------------------
    # SUMMARY
    # -------------------------------
    print("📊 Cleaning Summary")
    print(f"   Success     : {success}")
    print(f"   Failed      : {failed}")
    print(f"   Total Rows  : {total_rows}")

    # -------------------------------
    # CROSS-SITE PATIENT DEDUP
    # -------------------------------
    patients_unified_removed = 0
    for part_key, frames in patient_frames.items():
        combined = pd.concat(frames, ignore_index=True)
        before_rows = len(combined)
        dedup_key = build_patient_dedup_key(combined)

        if dedup_key is None:
            unified = combined.drop_duplicates()
        else:
            unified = combined.copy()
            unified["_dedup_key"] = dedup_key
            unified["_non_nulls"] = unified.notna().sum(axis=1)
            unified = unified.sort_values("_non_nulls", ascending=False)
            unified = unified.drop_duplicates(subset="_dedup_key", keep="first")
            unified = unified.drop(columns=["_dedup_key", "_non_nulls"], errors="ignore")

        after_rows = len(unified)
        removed = max(before_rows - after_rows, 0)
        patients_unified_removed += removed

        output_dir = REFINED_DIR / "patients"
        if part_key != "unpartitioned":
            output_dir = output_dir / part_key
        output_dir.mkdir(parents=True, exist_ok=True)
        save_clean(unified, str(output_dir.relative_to(REFINED_DIR)), "patients_unified.parquet")
        print(f"✅ Patients unified saved → {output_dir}/patients_unified.parquet")

    if patient_frames:
        metrics["patients_unified"] = {
            "nulls_handled": 0,
            "duplicates_removed": int(patients_unified_removed),
            "encoding_fixed": 0,
        }

    LOG_QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(f"✅ Cleaning metrics saved → {METRICS_PATH}")

    with open(FILE_METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(file_metrics, f, indent=2)
    print(f"✅ Cleaning file metrics saved → {FILE_METRICS_PATH}")

    processing_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    for dataset_name in sorted(dataset_totals.keys()):
        totals = dataset_totals[dataset_name]
        print(
            json.dumps(
                {
                    "dataset": dataset_name,
                    "rows_in": totals["rows_in"],
                    "rows_out": totals["rows_out"],
                    "issues_found": {
                        "duplicates_removed": totals["duplicates_removed"],
                        "nulls_handled": totals["nulls_handled"],
                        "encoding_fixed": totals["encoding_fixed"],
                    },
                    "processing_timestamp": processing_ts,
                }
            )
        )


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run_cleaning()
