import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
import pandas as pd

# -------------------------------
# CONFIG
# -------------------------------
RAW_DIR = Path("datalake/raw")
REFINED_DIR = Path("datalake/refined")
CONSUMPTION_DIR = Path("datalake/consumption")


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def file_metadata(root: Path, path: Path) -> dict:
    rel = path.relative_to(root).as_posix()
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)

    entry = {
        "file": rel,
        "row_count": None,
        "schema": None,
        "timestamp": mtime.strftime("%Y-%m-%d %H:%M:%S"),
        "sha256": sha256_file(path),
    }

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
        entry["row_count"] = int(len(df))
        entry["schema"] = list(df.columns)

    return entry


def write_manifest(root: Path):
    manifest_path = root / "manifest.json"
    entries = []

    for path in sorted(root.rglob("*")):
        if not path.is_file():
            continue
        if path.name == "manifest.json":
            continue
        entries.append(file_metadata(root, path))

    output = {
        "root": root.as_posix(),
        "generated": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "files": entries,
    }

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    print(f"✅ Manifest saved → {manifest_path}")


def run_manifests():
    write_manifest(RAW_DIR)
    write_manifest(REFINED_DIR)
    write_manifest(CONSUMPTION_DIR)


if __name__ == "__main__":
    run_manifests()
