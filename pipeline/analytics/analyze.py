import json
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone
import re

# -------------------------------
# CONFIG
# -------------------------------
CONSUMPTION_DIR = Path("datalake/consumption/v1")
ANALYTICS_DIR = Path("datalake/reports/analytics")
PARTITION_PREFIX = "ingest_date="


# -------------------------------
# SAFE NUMERIC STATS
# -------------------------------
def safe_numeric_stats(series: pd.Series, decimals: int = 2) -> dict:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {"mean": None, "median": None, "min": None, "max": None, "count": 0}
    return {
        "mean":   round(float(s.mean()),   decimals),
        "median": round(float(s.median()), decimals),
        "min":    round(float(s.min()),    decimals),
        "max":    round(float(s.max()),    decimals),
        "count":  int(s.count()),
    }


def stringify_keys(data: dict) -> dict:
    return {str(k): v for k, v in data.items()}


def safe_name(name: str) -> str:
    name = name.lower()
    name = re.sub(r"[^a-z0-9_.-]+", "_", name)
    return name.replace(".", "_").strip("_")


def icd10_chapter(code: str) -> str:
    if not code or pd.isna(code):
        return "unknown"

    code = str(code).upper()
    letter = code[0]
    num = None
    try:
        num = int(re.sub(r"[^0-9]", "", code)[0:2])
    except Exception:
        num = None

    if letter == "A" or letter == "B":
        return "Infectious and parasitic"
    if letter == "C":
        return "Neoplasms"
    if letter == "D" and num is not None and num <= 48:
        return "Neoplasms"
    if letter == "D":
        return "Blood/immune"
    if letter == "E":
        return "Endocrine/metabolic"
    if letter == "F":
        return "Mental/behavioral"
    if letter == "G":
        return "Nervous system"
    if letter == "H" and num is not None and num <= 59:
        return "Eye"
    if letter == "H":
        return "Ear"
    if letter == "I":
        return "Circulatory"
    if letter == "J":
        return "Respiratory"
    if letter == "K":
        return "Digestive"
    if letter == "L":
        return "Skin"
    if letter == "M":
        return "Musculoskeletal"
    if letter == "N":
        return "Genitourinary"
    if letter == "O":
        return "Pregnancy/childbirth"
    if letter == "P":
        return "Perinatal"
    if letter == "Q":
        return "Congenital"
    if letter == "R":
        return "Symptoms/signs"
    if letter == "S" or letter == "T":
        return "Injury/poisoning"
    if letter == "V" or letter == "W" or letter == "X" or letter == "Y":
        return "External causes"
    if letter == "Z":
        return "Health services"

    return "unknown"


def age_distribution(df: pd.DataFrame) -> dict:
    dob_col = next((c for c in ["date_of_birth", "birthdate", "dob"] if c in df.columns), None)
    if not dob_col:
        return {}

    dob = pd.to_datetime(df[dob_col], errors="coerce")
    dob = dob.dt.tz_localize(None)
    today = pd.Timestamp.now("UTC").normalize().tz_localize(None)
    age = ((today - dob).dt.days / 365.25).astype("float")
    age = age.where(age.between(0, 120))

    bins = [0, 18, 30, 45, 60, 75, 120]
    labels = ["0-17", "18-29", "30-44", "45-59", "60-74", "75+"]
    bucketed = pd.cut(age, bins=bins, labels=labels, right=False)
    return stringify_keys(bucketed.value_counts(dropna=False).to_dict())


# -------------------------------
# NOTES ANALYTICS
# -------------------------------
def analyze_notes(df: pd.DataFrame) -> dict:
    result = {}

    if "note_category" in df.columns:
        result["top_note_categories"] = stringify_keys(
            df["note_category"].astype(str).str.lower().value_counts().head(10).to_dict()
        )

    if "author" in df.columns:
        result["notes_per_author"] = stringify_keys(
            df["author"].value_counts().head(10).to_dict()
        )

    if "word_count" in df.columns:
        result["word_count_stats"] = safe_numeric_stats(df["word_count"], 1)

    if "is_addendum" in df.columns:
        col = df["is_addendum"]
        addendum_count = int(col.astype(str).str.upper().eq("Y").sum())
        result["addendum_count"] = addendum_count
        if len(df) > 0:
            result["addendum_rate_pct"] = round((addendum_count / len(df)) * 100, 1)
        else:
            result["addendum_rate_pct"] = 0.0

    return result


# -------------------------------
# DIAGNOSES ANALYTICS
# -------------------------------
def analyze_diagnoses(df: pd.DataFrame) -> dict:
    result = {}

    if "icd10_code" in df.columns:
        result["top_10_diagnoses"] = stringify_keys(
            df["icd10_code"].value_counts().head(10).to_dict()
        )

        chapters = df["icd10_code"].apply(icd10_chapter)
        result["icd10_chapter_distribution"] = stringify_keys(
            chapters.value_counts(dropna=False).to_dict()
        )

    if "severity" in df.columns:
        result["severity_distribution"] = stringify_keys(
            df["severity"].astype(str).str.lower().value_counts(dropna=False).to_dict()
        )

    if "status" in df.columns:
        result["status_distribution"] = stringify_keys(
            df["status"].astype(str).str.lower().value_counts().to_dict()
        )

    if "is_primary" in df.columns:
        result["primary_vs_secondary"] = stringify_keys(
            df["is_primary"].value_counts().to_dict()
        )

    return result


# -------------------------------
# VARIANTS ANALYTICS
# -------------------------------
def analyze_variants(df: pd.DataFrame) -> dict:
    result = {}

    if "clinical_significance" in df.columns:
        col = df["clinical_significance"].astype(str).str.lower()

        result["clinical_significance_distribution"] = stringify_keys(
            col.value_counts(dropna=False).to_dict()
        )

        high_risk = {"pathogenic", "likely pathogenic"}
        result["high_risk_variant_count"] = int(col.isin(high_risk).sum())

    if "gene" in df.columns:
        result["top_10_mutated_genes"] = stringify_keys(
            df["gene"].value_counts().head(10).to_dict()
        )

    if "variant_type" in df.columns:
        result["variant_type_distribution"] = stringify_keys(
            df["variant_type"].astype(str).str.lower().value_counts().to_dict()
        )

    if "allele_frequency" in df.columns:
        result["allele_frequency_stats"] = safe_numeric_stats(df["allele_frequency"], 4)

    return result


# -------------------------------
# LABS ANALYTICS
# -------------------------------
def analyze_labs(df: pd.DataFrame) -> dict:
    result = {}

    if "test_name" in df.columns:
        result["test_distribution"] = stringify_keys(
            df["test_name"].value_counts().to_dict()
        )

    if "test_value" in df.columns:
        tv = pd.to_numeric(df["test_value"], errors="coerce")
        result["missing_test_values"] = int(tv.isna().sum())

        if "test_name" in df.columns:
            outliers = {}
            for test, group in df.groupby("test_name"):
                values = pd.to_numeric(group["test_value"], errors="coerce").dropna()
                if values.empty:
                    continue
                q1 = values.quantile(0.25)
                q3 = values.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                count = int(((values < lower) | (values > upper)).sum())
                if count > 0:
                    outliers[str(test)] = count

            result["out_of_range_by_test"] = stringify_keys(outliers)

    return result


# -------------------------------
# PATIENTS ANALYTICS
# -------------------------------
def analyze_patients(df: pd.DataFrame) -> dict:
    result = {}

    gender_col = next((c for c in ["sex", "gender"] if c in df.columns), None)
    if gender_col:
        result["gender_distribution"] = stringify_keys(
            df[gender_col].astype(str).str.lower().value_counts(dropna=False).to_dict()
        )

    blood_col = next((c for c in ["blood_group", "bloodtype"] if c in df.columns), None)
    if blood_col:
        result["blood_group_distribution"] = stringify_keys(
            df[blood_col].value_counts(dropna=False).to_dict()
        )

    if "site" in df.columns:
        result["patients_per_site"] = stringify_keys(
            df["site"].value_counts().to_dict()
        )

    age_dist = age_distribution(df)
    if age_dist:
        result["age_distribution"] = age_dist

    return result


# -------------------------------
# MEDICATIONS ANALYTICS
# -------------------------------
def analyze_medications(df: pd.DataFrame) -> dict:
    result = {}

    if "medication_name" in df.columns:
        result["top_10_medications"] = stringify_keys(
            df["medication_name"].value_counts().head(10).to_dict()
        )

    if "status" in df.columns:
        result["status_distribution"] = stringify_keys(
            df["status"].astype(str).str.lower().value_counts().to_dict()
        )

    return result


def analyze_unified(df: pd.DataFrame) -> dict:
    result = {}

    if "site" in df.columns:
        result["site_distribution"] = stringify_keys(
            df["site"].value_counts(dropna=False).to_dict()
        )

    age_dist = age_distribution(df)
    if age_dist:
        result["age_distribution"] = age_dist

    if "high_risk_variants_count" in df.columns:
        result["patients_with_high_risk_variants"] = int(
            pd.to_numeric(df["high_risk_variants_count"], errors="coerce").fillna(0).gt(0).sum()
        )

    if "abnormal_lab_count" in df.columns:
        result["patients_with_abnormal_labs"] = int(
            pd.to_numeric(df["abnormal_lab_count"], errors="coerce").fillna(0).gt(0).sum()
        )

    if "high_risk_patient" in df.columns:
        result["high_risk_patient_count"] = int(df["high_risk_patient"].fillna(False).astype(bool).sum())
    else:
        result["high_risk_patient_count"] = 0

    return result


# -------------------------------
# ROUTER
# -------------------------------
ANALYZERS = {
    "notes": analyze_notes,
    "diagnoses": analyze_diagnoses,
    "variants": analyze_variants,
    "labs": analyze_labs,
    "patients": analyze_patients,
    "medications": analyze_medications,
    "unified": analyze_unified,
}


def latest_partition_dir(dataset_dir: Path) -> Path | None:
    if not dataset_dir.exists():
        return None

    parts = sorted([p for p in dataset_dir.iterdir() if p.is_dir() and p.name.startswith(PARTITION_PREFIX)])
    return parts[-1] if parts else dataset_dir


def latest_file(dataset: str, filename_hint: str | None = None) -> Path | None:
    dataset_dir = CONSUMPTION_DIR / dataset
    part_dir = latest_partition_dir(dataset_dir)
    if part_dir is None:
        return None

    files = list(part_dir.glob("*.parquet"))
    if not files:
        return None

    if filename_hint:
        match = next((f for f in files if filename_hint in f.name), None)
        if match:
            return match

    return sorted(files)[-1]


def partition_from_path(path: Path) -> str:
    parts = path.parts
    part = next((p for p in parts if p.startswith(PARTITION_PREFIX)), None)
    return part or "unpartitioned"


def save_parquet_output(df: pd.DataFrame, name: str, partition: str):
    output_dir = CONSUMPTION_DIR / "analytics"
    if partition != "unpartitioned":
        output_dir = output_dir / partition
    output_dir.mkdir(parents=True, exist_ok=True)

    out_path = output_dir / f"{name}.parquet"
    tmp_path = output_dir / f"{name}.tmp"
    df.to_parquet(tmp_path, index=False)
    tmp_path.replace(out_path)
    print(f"   Saved → {out_path}")


# -------------------------------
# SAVE
# -------------------------------
def save_analytics(data: dict, dataset: str, row_count: int, source_file: str, generated_ts: str | None):
    output = {
        "_metadata": {
            "dataset": dataset,
            "row_count": row_count,
            "generated": generated_ts or "unknown",
            "source_file": source_file,
        },
        "analytics": data,
    }

    safe_source = safe_name(source_file)
    out_path = ANALYTICS_DIR / f"{dataset}_{safe_source}_analytics.json"

    tmp_path = ANALYTICS_DIR / f"{dataset}_{safe_source}_analytics.json.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, default=str)

    tmp_path.replace(out_path)

    print(f"   Saved → {out_path}")


# -------------------------------
# MAIN
# -------------------------------
def run_analytics():
    print("📊 Starting Analytics...\n")

    ANALYTICS_DIR.mkdir(parents=True, exist_ok=True)

    files = list(CONSUMPTION_DIR.glob(f"**/{PARTITION_PREFIX}*/*.parquet"))
    if not files:
        files = list(CONSUMPTION_DIR.glob("*/*.parquet"))

    success, skipped = 0, 0

    for file in sorted(files):
        parts = file.parts
        dataset_name = parts[parts.index("v1") + 1] if "v1" in parts else file.parent.name

        print(f"📂 Analyzing: {dataset_name}/{file.name}")

        df = pd.read_parquet(file)

        if df.empty:
            print("   ⚠ Empty file — skipped\n")
            skipped += 1
            continue

        analyzer = ANALYZERS.get(dataset_name)

        if analyzer is None:
            print(f"   ⚠ No analyzer for '{dataset_name}' — skipped\n")
            skipped += 1
            continue

        result = analyzer(df)

        generated_ts = None
        if "_ingestion_time" in df.columns:
            try:
                generated_ts = str(pd.to_datetime(df["_ingestion_time"], errors="coerce").max())
            except Exception:
                generated_ts = None

        print(f"   Rows    : {len(df)}")
        print(f"   Metrics : {len(result)} computed")

        save_analytics(result, dataset_name, len(df), file.name, generated_ts)
        print()

        success += 1

    print("📊 Analytics Summary")
    print(f"   Completed : {success}")
    print(f"   Skipped   : {skipped}")
    print("   Output    → datalake/reports/analytics/")

    print("\n📦 Writing Task 3 parquet outputs...\n")

    unified_file = latest_file("unified", "patients_unified_analytics")
    labs_file = latest_file("labs")
    diag_file = latest_file("diagnoses")
    variants_file = latest_file("variants")

    partition = "unpartitioned"
    if unified_file:
        partition = partition_from_path(unified_file)

    # Patient demographics summary
    if unified_file:
        df = pd.read_parquet(unified_file)
        rows = []

        for bucket, count in age_distribution(df).items():
            rows.append({"metric": "age_distribution", "category": bucket, "count": int(count)})

        gender_col = next((c for c in ["sex", "gender"] if c in df.columns), None)
        if gender_col:
            counts = df[gender_col].astype("string").str.lower().value_counts(dropna=False)
            for k, v in counts.items():
                rows.append({"metric": "gender_distribution", "category": str(k), "count": int(v)})

        if "site" in df.columns:
            counts = df["site"].value_counts(dropna=False)
            for k, v in counts.items():
                rows.append({"metric": "site_distribution", "category": str(k), "count": int(v)})

        patient_summary = pd.DataFrame(rows)
        save_parquet_output(patient_summary, "patient_summary", partition)

    # Lab statistics + trends
    if labs_file:
        labs = pd.read_parquet(labs_file)
        labs["test_value"] = pd.to_numeric(labs.get("test_value"), errors="coerce")

        stats = []
        if "test_name" in labs.columns:
            for test, group in labs.groupby("test_name"):
                values = pd.to_numeric(group["test_value"], errors="coerce").dropna()
                if values.empty:
                    continue
                q1 = values.quantile(0.25)
                q3 = values.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr
                out_count = int(((values < lower) | (values > upper)).sum())

                stats.append({
                    "record_type": "stat",
                    "test_name": str(test),
                    "mean": float(values.mean()),
                    "median": float(values.median()),
                    "std": float(values.std()),
                    "count": int(values.count()),
                    "out_of_range_count": out_count,
                    "period": None,
                })

        trends = []
        if "test_name" in labs.columns and "collection_date" in labs.columns:
            labs["collection_date"] = pd.to_datetime(labs["collection_date"], errors="coerce")
            labs["period"] = labs["collection_date"].dt.to_period("M").astype("string")
            for test in ["hba1c", "creatinine"]:
                subset = labs[labs["test_name"].astype("string").str.lower().str.contains(test, na=False)]
                if subset.empty:
                    continue
                grp = subset.groupby("period", dropna=True)["test_value"].mean().dropna()
                for period, mean_val in grp.items():
                    trends.append({
                        "record_type": "trend",
                        "test_name": test,
                        "mean": float(mean_val),
                        "median": None,
                        "std": None,
                        "count": None,
                        "out_of_range_count": None,
                        "period": str(period),
                    })

        lab_stats = pd.DataFrame(stats + trends)
        save_parquet_output(lab_stats, "lab_statistics", partition)

    # Diagnosis frequency (top 15 chapters by patient count)
    if diag_file:
        diag = pd.read_parquet(diag_file)
        if "icd10_code" in diag.columns and "patient_id" in diag.columns:
            chapters = diag["icd10_code"].apply(icd10_chapter)
            diag = diag.assign(chapter=chapters)
            counts = diag.groupby("chapter")["patient_id"].nunique().sort_values(ascending=False).head(15)
            diagnosis_frequency = counts.reset_index().rename(columns={"patient_id": "patient_count"})
            save_parquet_output(diagnosis_frequency, "diagnosis_frequency", partition)

    # Variant hotspots (top 5 genes, pathogenic only)
    if variants_file:
        variants = pd.read_parquet(variants_file)
        if "clinical_significance" in variants.columns and "gene" in variants.columns:
            cs = variants["clinical_significance"].astype("string").str.lower()
            variants = variants[cs.isin({"pathogenic", "likely pathogenic"})]
            if not variants.empty and "allele_frequency" in variants.columns:
                variants["allele_frequency"] = pd.to_numeric(variants["allele_frequency"], errors="coerce")
                grouped = variants.groupby("gene")["allele_frequency"].agg([
                    ("count", "count"),
                    ("mean", "mean"),
                    ("median", "median"),
                    ("p25", lambda x: float(x.quantile(0.25))),
                    ("p75", lambda x: float(x.quantile(0.75))),
                    ("min", "min"),
                    ("max", "max"),
                ])
                grouped = grouped.sort_values(by="count", ascending=False).head(5).reset_index()
                save_parquet_output(grouped, "variant_hotspots", partition)

    # High-risk patients (hba1c > 7.0 AND pathogenic variant)
    if labs_file and variants_file:
        labs = pd.read_parquet(labs_file)
        variants = pd.read_parquet(variants_file)

        if "test_value" in labs.columns:
            labs["test_value"] = pd.to_numeric(labs["test_value"], errors="coerce")
        else:
            labs["test_value"] = pd.NA

        if "test_name" in labs.columns:
            hba = labs[labs["test_name"].astype("string").str.lower().str.contains("hba1c", na=False)]
            hba = hba[hba["test_value"] > 7.0]
        else:
            hba = labs.iloc[0:0]

        if "clinical_significance" in variants.columns:
            cs = variants["clinical_significance"].astype("string").str.lower()
            path = variants[cs.isin({"pathogenic", "likely pathogenic"})]
        else:
            path = variants.iloc[0:0]

        high_risk_ids = set(hba.get("patient_id", [])) & set(path.get("patient_id", []))
        high_risk_df = pd.DataFrame({"patient_id": list(high_risk_ids)})

        if unified_file and not high_risk_df.empty:
            unified = pd.read_parquet(unified_file)
            high_risk_df = high_risk_df.merge(unified, on="patient_id", how="left")

        save_parquet_output(high_risk_df, "high_risk_patients", partition)


if __name__ == "__main__":
    run_analytics()

