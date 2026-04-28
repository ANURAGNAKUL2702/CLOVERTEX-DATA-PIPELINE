import json
import pandas as pd
from pathlib import Path
  
# -------------------------------
# CONFIG
# -------------------------------
RAW_DIR = Path("datalake/raw")
REFINED_DIR = Path("datalake/refined/v1")
LOG_QUALITY_DIR = Path("logs/quality")
METRICS_PATH = LOG_QUALITY_DIR / "cleaning_metrics.json"
PARTITION_PREFIX = "ingest_date="


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


def iter_raw_files():
    if not RAW_DIR.exists():
        raise FileNotFoundError(f"Raw dir not found: {RAW_DIR}")

    files = list(RAW_DIR.glob(f"**/{PARTITION_PREFIX}*/*.parquet"))

    # Fallback for non-partitioned layout
    if not files:
        files = list(RAW_DIR.glob("*/*.parquet"))

    return files


# -------------------------------
# MAIN CLEANING PIPELINE
# -------------------------------
def run_cleaning():
    print("🧹 Starting Cleaning...\n")

    raw_files = iter_raw_files()
    success, failed, total_rows = 0, 0, 0
    metrics: dict[str, dict[str, int]] = {}
    patient_frames: dict[str, list[pd.DataFrame]] = {}

    for file in raw_files:
        try:
            # raw/<dataset>/ingest_date=YYYY-MM-DD/<file>
            parts = file.parts
            dataset_name = parts[parts.index("raw") + 1] if "raw" in parts else file.parent.name
            ingest_partition = next((p for p in parts if p.startswith(PARTITION_PREFIX)), None)

            print(f"📂 Cleaning: {dataset_name}/{file.name}")

            df = pd.read_parquet(file)

            before_rows, before_cols = df.shape
            before_missing = int(df.isna().sum().sum())

            # Clean
            df = clean_dataframe(df)
            after_missing = int(df.isna().sum().sum())

            df = df.drop_duplicates()
            after_rows, after_cols = df.shape
            total_rows += after_rows

            nulls_handled = max(after_missing - before_missing, 0)
            duplicates_removed = max(before_rows - after_rows, 0)

            metrics_entry = metrics.setdefault(dataset_name, {"nulls_handled": 0, "duplicates_removed": 0})
            metrics_entry["nulls_handled"] += int(nulls_handled)
            metrics_entry["duplicates_removed"] += int(duplicates_removed)

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
                    save_clean(group, str(partition_dir.relative_to(REFINED_DIR)), file.name)
                saved_message = f"{REFINED_DIR / dataset_name} (partitioned by test_name)"
            else:
                output_dir.mkdir(parents=True, exist_ok=True)
                save_clean(df, str(output_dir.relative_to(REFINED_DIR)), file.name)
                saved_message = str(output_dir)

            stats = {
                "dataset": dataset_name,
                "rows_before": int(before_rows),
                "rows_after": int(after_rows),
                "nulls_handled": int(nulls_handled),
                "duplicates_removed": int(duplicates_removed),
            }

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
        }

    LOG_QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    with open(METRICS_PATH, "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)
    print(f"✅ Cleaning metrics saved → {METRICS_PATH}")


# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run_cleaning()
    
