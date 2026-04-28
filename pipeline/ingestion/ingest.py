import hashlib
import shutil
from datetime import datetime, timezone
from pathlib import Path

# -------------------------------
# CONFIG
# -------------------------------
INPUT_DIR = Path("data")
RAW_DIR = Path("datalake/raw")
SUPPORTED_EXT = [".csv", ".xlsx", ".xls", ".parquet", ".json"]


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


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
# COPY RAW (PARTITION + ATOMIC WRITE)
# -------------------------------
def ingest_partition_from_mtime(path: Path) -> str:
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    return mtime.strftime("%Y-%m-%d")


def copy_to_raw(source: Path, dataset: str) -> tuple[Path, bool]:
    ingest_date = ingest_partition_from_mtime(source)
    output_path = RAW_DIR / dataset / f"ingest_date={ingest_date}"
    output_path.mkdir(parents=True, exist_ok=True)

    final_file = output_path / source.name

    if final_file.exists():
        if sha256_file(final_file) == sha256_file(source):
            return final_file, True

    temp_file = output_path / f"{source.name}.tmp"
    shutil.copy2(source, temp_file)
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

    files = [
        f for f in INPUT_DIR.rglob("*")
        if f.is_file() and f.suffix.lower() in SUPPORTED_EXT
    ]

    if not files:
        print("❌ No files found in data/raw_input/")
        return

    success, failed, skipped = 0, 0, 0

    for file in files:
        try:
            print(f"[INFO] Processing: {file.name}")

            # -------------------------------
            # DATASET BUCKET
            # -------------------------------
            dataset = get_dataset_bucket(file.name)

            # -------------------------------
            # COPY RAW (UNTOUCHED)
            # -------------------------------
            output_file, already_exists = copy_to_raw(file, dataset)

            print("[INFO] Raw copy created")
            if already_exists:
                print(f"[INFO] Skipped (already exists) → {output_file}")
                skipped += 1
            else:
                print(f"[INFO] Saved → {output_file}")

            print()

            if not already_exists:
                success += 1

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




# -------------------------------
# ENTRY POINT
# -------------------------------
if __name__ == "__main__":
    run_ingestion()