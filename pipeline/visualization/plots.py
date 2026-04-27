import matplotlib
matplotlib.use("Agg")

import json
import pandas as pd
from pathlib import Path
import matplotlib.pyplot as plt

# -------------------------------
# CONFIG
# -------------------------------
CONSUMPTION_DIR = Path("datalake/consumption/v1")
PLOTS_DIR = Path("datalake/consumption/plots")
PARTITION_PREFIX = "ingest_date="
QUALITY_REPORT = Path("logs/quality/data_quality_report.json")


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


def latest_analytics_file(filename: str) -> Path | None:
    analytics_dir = CONSUMPTION_DIR / "analytics"
    part_dir = latest_partition_dir(analytics_dir)
    if part_dir is None:
        return None

    candidate = part_dir / f"{filename}.parquet"
    return candidate if candidate.exists() else None


def save_plot(fig, name: str):
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out_path = PLOTS_DIR / f"{name}.png"
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)
    print(f"✅ Saved plot → {out_path}")


def plot_age_histogram():
    file = latest_file("unified", "patients_unified_analytics")
    if file is None:
        print("⚠ No unified dataset found for age histogram")
        return

    df = pd.read_parquet(file)
    dob_col = next((c for c in ["date_of_birth", "birthdate", "dob"] if c in df.columns), None)
    if not dob_col:
        print("⚠ No DOB column found for age histogram")
        return

    dob = pd.to_datetime(df[dob_col], errors="coerce")
    today = pd.Timestamp.now("UTC").normalize().tz_localize(None)
    age = ((today - dob.dt.tz_localize(None)).dt.days / 365.25).astype("float")
    age = age.where(age.between(0, 120))

    fig, ax = plt.subplots(figsize=(8, 4.5))
    ax.hist(age.dropna(), bins=20, color="#4C78A8", edgecolor="white")
    ax.set_title("Age Distribution")
    ax.set_xlabel("Age")
    ax.set_ylabel("Count")
    save_plot(fig, "age_histogram")


def plot_gender_bar():
    file = latest_file("unified", "patients_unified_analytics")
    if file is None:
        print("⚠ No unified dataset found for gender bar chart")
        return

    df = pd.read_parquet(file)
    gender_col = next((c for c in ["sex", "gender"] if c in df.columns), None)
    if not gender_col:
        print("⚠ No gender column found for gender bar chart")
        return

    counts = df[gender_col].astype("string").str.lower().value_counts(dropna=False)
    fig, ax = plt.subplots(figsize=(6, 4))
    counts.plot(kind="bar", ax=ax, color="#F58518")
    ax.set_title("Gender Distribution")
    ax.set_xlabel("Gender")
    ax.set_ylabel("Count")
    save_plot(fig, "gender_bar")


def plot_diagnosis_frequency():
    file = latest_analytics_file("diagnosis_frequency")
    if file is None:
        print("⚠ No diagnosis_frequency parquet found")
        return

    df = pd.read_parquet(file)
    if not {"chapter", "patient_count"}.issubset(set(df.columns)):
        print("⚠ diagnosis_frequency schema missing")
        return

    df = df.sort_values("patient_count", ascending=True).tail(15)
    fig, ax = plt.subplots(figsize=(9, 5.5))
    ax.barh(df["chapter"], df["patient_count"], color="#54A24B")
    ax.set_title("Top 15 ICD-10 Chapters by Patient Count")
    ax.set_xlabel("Patient Count")
    ax.set_ylabel("ICD-10 Chapter")
    save_plot(fig, "diagnosis_frequency_chapters")


def plot_lab_distribution():
    file = latest_file("labs")
    if file is None:
        print("⚠ No labs dataset found for lab distribution")
        return

    df = pd.read_parquet(file)
    if not {"test_name", "test_value"}.issubset(set(df.columns)):
        print("⚠ test_name/test_value not found for lab distribution")
        return

    # Reference ranges for two common tests
    ref_ranges = {
        "hba1c": (4.0, 5.7),
        "creatinine": (0.7, 1.3),
    }

    fig, axes = plt.subplots(1, 2, figsize=(10, 4.5))
    for ax, (test, (low, high)) in zip(axes, ref_ranges.items()):
        subset = df[df["test_name"].astype("string").str.lower().str.contains(test, na=False)]
        values = pd.to_numeric(subset["test_value"], errors="coerce").dropna()
        ax.hist(values, bins=20, color="#72B7B2", edgecolor="white")
        ax.axvline(low, color="#E45756", linestyle="--", linewidth=1)
        ax.axvline(high, color="#E45756", linestyle="--", linewidth=1)
        ax.set_title(f"{test.upper()} distribution")
        ax.set_xlabel("Value")
        ax.set_ylabel("Count")

    save_plot(fig, "lab_distribution_reference")


def plot_genomics_scatter():
    file = latest_file("variants")
    if file is None:
        print("⚠ No variants dataset found for genomics scatter")
        return

    df = pd.read_parquet(file)
    required = {"allele_frequency", "read_depth", "clinical_significance"}
    if not required.issubset(set(df.columns)):
        print("⚠ allele_frequency/read_depth not found for genomics scatter")
        return

    af = pd.to_numeric(df["allele_frequency"], errors="coerce")
    rd = pd.to_numeric(df["read_depth"], errors="coerce")

    cs = df["clinical_significance"].astype("string").str.lower()
    palette = {
        "pathogenic": "#E45756",
        "likely pathogenic": "#F58518",
        "uncertain significance": "#4C78A8",
        "benign": "#54A24B",
    }
    colors = cs.map(palette).fillna("#B279A2")

    fig, ax = plt.subplots(figsize=(7, 4.5))
    ax.scatter(af, rd, s=14, alpha=0.6, color=colors)
    ax.set_title("Genomics: Allele Frequency vs Read Depth")
    ax.set_xlabel("Allele Frequency")
    ax.set_ylabel("Read Depth")
    save_plot(fig, "genomics_scatter_significance")


def plot_high_risk_summary():
    file = latest_analytics_file("high_risk_patients")
    unified_file = latest_file("unified", "patients_unified_analytics")
    if file is None or unified_file is None:
        print("⚠ Missing data for high-risk summary")
        return

    high_risk = pd.read_parquet(file)
    total = pd.read_parquet(unified_file)
    high_count = int(len(high_risk))
    total_count = int(len(total))
    low_count = max(total_count - high_count, 0)

    fig, ax = plt.subplots(figsize=(5.5, 4))
    ax.bar(["High-risk", "Other"], [high_count, low_count], color=["#E45756", "#4C78A8"])
    ax.set_title("High-Risk Patient Summary")
    ax.set_ylabel("Patient Count")
    save_plot(fig, "high_risk_summary")


def plot_data_quality_overview():
    if not QUALITY_REPORT.exists():
        print("⚠ data_quality_report.json not found")
        return

    with open(QUALITY_REPORT, "r", encoding="utf-8") as f:
        report = json.load(f)

    cleaning = report.get("cleaning", {})
    nulls = sum(v.get("nulls_handled", 0) for v in cleaning.values())
    dups = sum(v.get("duplicates_removed", 0) for v in cleaning.values())
    orphans = sum(report.get("orphan_records", {}).values())
    schema_mismatches = len(report.get("schema_mismatches", {}))

    metrics = {
        "nulls_handled": nulls,
        "duplicates_removed": dups,
        "orphan_records": orphans,
        "schema_mismatches": schema_mismatches,
    }

    fig, ax = plt.subplots(figsize=(7, 4))
    ax.bar(metrics.keys(), metrics.values(), color="#9C755F")
    ax.set_title("Data Quality Overview")
    ax.set_ylabel("Count")
    ax.tick_params(axis="x", rotation=20)
    save_plot(fig, "data_quality_overview")


def run_visualization():
    print("📈 Starting Visualization...\n")
    plot_age_histogram()
    plot_gender_bar()
    plot_diagnosis_frequency()
    plot_lab_distribution()
    plot_genomics_scatter()
    plot_high_risk_summary()
    plot_data_quality_overview()


if __name__ == "__main__":
    run_visualization()
