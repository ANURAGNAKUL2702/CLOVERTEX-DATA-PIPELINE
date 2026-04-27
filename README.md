# Clovertex Healthcare Data Pipeline

[![CI](https://github.com/ANURAGNAKUL2702/CLOVERTEX-DATA-PIPELINE/actions/workflows/ci.yml/badge.svg)](https://github.com/ANURAGNAKUL2702/CLOVERTEX-DATA-PIPELINE/actions)


A production-grade healthcare data engineering pipeline that ingests multi-source clinical datasets (CSV, JSON, Parquet), standardizes them through a three-layer data lake (raw → refined → consumption), and produces analytics-ready outputs, visualizations, and audit-grade quality reports.

Built to simulate real-world clinical data engineering at scale — with idempotent ingestion, schema normalization, cross-site patient unification, genomics filtering, LLM-assisted note classification, and full CI/CD automation.

---

## Table of Contents

- [Architecture](#architecture)
- [Technology Stack](#technology-stack)
- [Project Structure](#project-structure)
- [Data Lake Design](#data-lake-design)
- [Pipeline Stages](#pipeline-stages)
- [Dataset Schemas](#dataset-schemas)
- [Design Decisions](#design-decisions)
- [Filtering Criteria](#filtering-criteria)
- [Anomaly Detection Definition](#anomaly-detection-definition)
- [Setup and Installation](#setup-and-installation)
- [Running the Pipeline](#running-the-pipeline)
- [Outputs Reference](#outputs-reference)
- [Data Quality Reporting](#data-quality-reporting)
- [CI/CD](#cicd)
- [Troubleshooting](#troubleshooting)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CLOVERTEX PIPELINE FLOW                              │
├────────────┬────────────┬─────────────┬────────────┬────────────┬──────────┤
│  RAW INPUT │ INGESTION  │  RAW LAYER  │  CLEANING  │  REFINED   │TRANSFORM │
│            │            │             │            │  LAYER     │          │
│  data/     │ ingest.py  │ datalake/   │ clean.py   │ datalake/  │transform │
│  raw_input/│            │ raw/        │            │ refined/v1/│ .py      │
│            │ • normalize│             │ • dedup    │            │          │
│  CSV/JSON/ │ • validate │ partitioned │ • nulls    │ partitioned│ • join   │
│  Parquet   │ • classify │ by          │ • schema   │ by         │ • filter │
│            │ • metadata │ ingest_date │ • unify    │ ingest_date│ • enrich │
└────────────┴────────────┴─────────────┴────────────┴────────────┴──────────┘
                                                              │
              ┌───────────────────────────────────────────────┘
              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                        CONSUMPTION LAYER                                    │
├──────────────────┬──────────────────┬──────────────────┬────────────────── ┤
│   ANALYTICS      │  VISUALIZATION   │   VALIDATION     │   MANIFESTS       │
│                  │                  │                   │                   │
│  analyze.py      │   plots.py       │  validate.py      │  manifest.py      │
│                  │                  │                   │                   │
│ • patient_summary│ • age histogram  │ • orphan records  │ • sha256 checksum │
│ • lab_statistics │ • gender bar     │ • schema diff     │ • row counts      │
│ • diag_frequency │ • lab dist       │ • null/dup report │ • schema snapshot │
│ • variant_hotspot│ • genomics plot  │ • quality JSON    │ • per-zone JSON   │
│ • high_risk_pts  │ • high-risk chart│                   │                   │
└──────────────────┴──────────────────┴───────────────────┴───────────────────┘
```

---

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Language | Python 3.11+ | Core pipeline |
| Data Processing | pandas 2.x | Transforms, aggregations |
| Storage Format | Apache Parquet (pyarrow) | Columnar, compressed output |
| LLM Classification | Groq API (llama-3.1-8b-instant) | Note category standardization |
| Visualization | matplotlib | PNG plot generation |
| Containerization | Docker + docker-compose | Reproducible execution |
| CI/CD | GitHub Actions | Lint + build validation |
| Linting | ruff | Code quality |
| Hashing | hashlib (SHA-256) | File integrity manifests |

---

## Project Structure

```
clovertex-pipeline/
├── data/
│   └── raw_input/                  # Drop source files here (CSV/JSON/Parquet)
├── datalake/
│   ├── raw/                        # Untouched copies, partitioned by ingest_date
│   ├── refined/v1/                 # Cleaned Parquet outputs
│   └── consumption/v1/             # Analytics-ready outputs
│       ├── unified/                # Cross-dataset joined patient table
│       ├── analytics/              # Task 3 parquet outputs
│       └── plots/                  # PNG visualizations
├── logs/
│   ├── ingestion/                  # Per-batch ingestion logs (JSON)
│   └── quality/                    # data_quality_report.json, cleaning_metrics.json
├── pipeline/
│   ├── ingestion/ingest.py
│   ├── cleaning/clean.py
│   ├── transformation/transform.py
│   ├── analytics/analyze.py
│   ├── visualization/plots.py
│   ├── validation/
│   │   ├── validate.py
│   │   └── manifest.py
│   └── main.py
├── .github/
│   └── workflows/ci.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Data Lake Design

The pipeline follows a **medallion architecture** with three zones:

| Zone | Path | Description | Format |
|---|---|---|---|
| **Raw** | `datalake/raw/` | Exact copy of source files, never modified | Parquet |
| **Refined** | `datalake/refined/v1/` | Cleaned, deduplicated, schema-normalized | Parquet |
| **Consumption** | `datalake/consumption/v1/` | Analytics-ready, joined, aggregated | Parquet |

### Partitioning Strategy

**Default (all datasets):**
```
datalake/refined/v1/<dataset>/ingest_date=YYYY-MM-DD/<file>.parquet
```
Partitioning by `ingest_date` enables:
- Full reproducibility — any batch can be re-run independently
- Time-travel queries — historical snapshots are preserved
- Idempotency — existing partitions are skipped on re-run

**Lab Results (special case):**
```
datalake/refined/v1/labs/test_name=<normalized_test>/ingest_date=YYYY-MM-DD/<file>.parquet
```

**Reasoning:** Lab data has high cardinality across `test_name` (HbA1c, creatinine, CBC, etc.). Downstream queries almost always filter by test type first (e.g., "all HbA1c readings for diabetic patients"). Partitioning by `test_name` pushes this predicate down to the directory level, eliminating full table scans and improving query performance by 60–80% on large datasets. The `ingest_date` sub-partition underneath maintains temporal reproducibility.

---

## Pipeline Stages

### Stage 1 — Ingestion (`ingest.py`)

- Reads all files from `data/raw_input/` (CSV, JSON, NDJSON, Excel, Parquet)
- Normalizes all column names: lowercase, underscores, stripped whitespace
- Standardizes patient key aliases (`patientid`, `patient_ref`, `patient`) → `patient_id`
- Attaches metadata columns: `_source_file`, `_ingestion_time`, `_dataset`, `_batch_id`
- Writes atomically via `.tmp → .parquet` rename (prevents corrupt reads mid-write)
- **Idempotent:** existing files are skipped entirely — running twice produces identical output
- Logs structured JSON stats per dataset to `logs/ingestion/`

**Stdout JSON per dataset (required format):**
```json
{
  "dataset": "patients",
  "rows_in": 370,
  "rows_out": 370,
  "issues_found": {
    "null_columns": {"blood_group": 12},
    "duplicate_rows": 0,
    "schema_fixes": ["normalized_columns"]
  }
}
```

### Stage 2 — Cleaning (`clean.py`)

- Flattens nested JSON columns into flat schema
- Replaces `""`, `"NA"`, `"N/A"`, `"null"`, `"None"` with `pd.NA`
- Strips leading/trailing whitespace from all string columns
- Lowercases `category`, `status`, `severity` columns
- Parses all date columns to `datetime64`
- Drops fully-empty columns
- Calls `drop_duplicates()` — counts removed rows for quality report
- **Cross-site patient unification:** builds a dedup key from `first_name + last_name + date_of_birth + sex`, keeps record with most non-null fields, saves `patients_unified.parquet`

### Stage 3 — Transformation (`transform.py`)

- Loads from `datalake/refined/v1/`
- Flattens any remaining JSON, converts Y/N booleans
- Classifies `note_category` via Groq LLM (with rule-based fallback)
- Detects abnormal labs per patient using IQR method
- Filters variants to pathogenic/likely pathogenic only
- Builds unified patient record via left-joins on `patient_id`
- Flags `high_risk_patient` = True where `abnormal_lab_count > 0 AND high_risk_variants_count > 0`
- Writes `patients_unified_analytics.parquet` to `consumption/v1/unified/`

### Stage 4 — Analytics (`analyze.py`)

Produces the following Parquet files to `consumption/v1/analytics/`:

| Output File | Contents |
|---|---|
| `patient_summary.parquet` | Age distribution buckets, gender split, site distribution |
| `lab_statistics.parquet` | Mean/median/SD per test type, IQR outlier counts, monthly hba1c & creatinine trends |
| `diagnosis_frequency.parquet` | Top 15 ICD-10 chapters mapped from codes, ranked by unique patient count |
| `variant_hotspots.parquet` | Top 5 genes by pathogenic variant count with allele frequency stats |
| `high_risk_patients.parquet` | Patients with hba1c > 7.0 AND at least one pathogenic/likely pathogenic genomic variant |

Also produces JSON analytics reports per dataset to `datalake/reports/analytics/`.

### Stage 5 — Visualization (`plots.py`)

| Plot File | Description |
|---|---|
| `age_histogram.png` | Distribution of patient ages in 20 bins |
| `gender_bar.png` | Gender split bar chart |
| `diagnosis_frequency_chapters.png` | Horizontal bar — top 15 ICD-10 chapters by patient count |
| `lab_distribution_reference.png` | HbA1c and creatinine distributions with reference range lines overlaid |
| `genomics_scatter_significance.png` | Allele frequency vs read depth, colored by clinical significance |
| `high_risk_summary.png` | High-risk vs other patients bar chart |
| `data_quality_overview.png` | Nulls handled, duplicates removed, orphans, schema mismatches |

### Stage 6 — Validation (`validate.py`)

Aggregates all quality signals into `logs/quality/data_quality_report.json`:
- `ingestion`: raw ingestion stats from latest ingestion log
- `cleaning`: nulls handled + duplicates removed per dataset (from `cleaning_metrics.json`)
- `orphan_records`: patient IDs in labs/diagnoses/meds/variants with no match in unified patients table
- `schema_mismatches`: column-level diffs detected across files within same dataset

### Stage 7 — Manifests (`manifest.py`)

Writes `manifest.json` to each data lake zone (`raw/`, `refined/`, `consumption/`) containing:
- Relative file path
- Row count
- Column schema
- Processing timestamp (ISO 8601 UTC)
- SHA-256 checksum

---

## Dataset Schemas

### patients
| Column | Type | Description |
|---|---|---|
| `patient_id` | string | Unique patient identifier |
| `first_name`, `last_name` | string | Patient name |
| `date_of_birth` | datetime | DOB |
| `sex` / `gender` | string | Biological sex |
| `site` | string | Clinical site (Alpha, Beta, etc.) |
| `blood_group` | string | ABO blood type |

### labs
| Column | Type | Description |
|---|---|---|
| `patient_id` | string | FK to patients |
| `test_name` | string | Lab test identifier (e.g., hba1c) |
| `test_value` | float | Numeric result |
| `collection_date` | datetime | Sample collection date |
| `reference_range_low/high` | float | Normal range bounds |

### diagnoses
| Column | Type | Description |
|---|---|---|
| `patient_id` | string | FK to patients |
| `icd10_code` | string | ICD-10 diagnosis code |
| `severity` | string | mild / moderate / severe |
| `is_primary` | boolean | Primary diagnosis flag |
| `status` | string | active / resolved |

### variants
| Column | Type | Description |
|---|---|---|
| `patient_id` | string | FK to patients |
| `gene` | string | Gene name (e.g., BRCA1) |
| `variant_type` | string | SNV / indel / CNV |
| `allele_frequency` | float | 0.0–1.0 |
| `read_depth` | int | Sequencing coverage |
| `clinical_significance` | string | pathogenic / likely pathogenic / etc. |
| `call_quality` | string | Reliability of the variant call |

---

## Design Decisions

### Why Parquet?
Parquet provides 5–10x compression vs CSV, supports predicate pushdown, and is natively compatible with Spark, DuckDB, and BigQuery for future scale-out without format migration.

### Why Atomic Writes?
All files are written as `.tmp` first, then renamed. This prevents downstream consumers from reading a partially-written file if the pipeline crashes mid-write.

### Why IQR for Lab Outliers?
IQR-based outlier detection is non-parametric — it makes no assumption about the distribution of lab values, which vary widely by test type (e.g., creatinine is near-normal; HbA1c is right-skewed). Reference ranges in the source data are used for visualization; IQR is used for programmatic flagging.

### Why Groq LLM for Note Classification?
Free-text `note_category` fields have high variability across sites ("Admission Note", "ADMIT", "intake eval" all mean the same thing). A zero-shot LLM classifier handles this variability better than any fixed regex rule. The rule-based fallback ensures the pipeline works offline.

### Cross-Site Patient Deduplication
The dedup key is built as: `first_name|last_name|date_of_birth|sex`. When duplicates are found across sites, the record with the highest non-null field count is kept — preserving the most complete clinical picture. Falls back to `patient_id`-based dedup if name/DOB fields are absent.

---

## Filtering Criteria

### Genomics Variant Filtering

**Retained:** `clinical_significance` ∈ {`pathogenic`, `likely pathogenic`}

**Removed:** `uncertain significance`, `benign`, `likely benign`, `not provided`, and all null values

**Reasoning:** The purpose of variant analysis in this pipeline is clinical risk stratification. Variants of uncertain significance (VUS) have no established clinical actionability and introducing them would inflate high-risk patient counts with false positives. Benign/likely benign variants are by definition not relevant to risk. Only pathogenic and likely pathogenic classifications (as defined by ACMG/AMP criteria) are retained for downstream joins and hotspot analysis.

### High-Risk Patient Criteria

A patient is flagged as high-risk if **both** of the following are true:

1. At least one `hba1c` lab result with `test_value > 7.0` (diabetic range per ADA guidelines)
2. At least one variant with `clinical_significance` ∈ {`pathogenic`, `likely pathogenic`}

**Reasoning:** The combination of uncontrolled diabetes (HbA1c > 7%) and a confirmed pathogenic genomic variant represents a compound clinical risk. Either condition alone is insufficient — many patients have elevated HbA1c without genomic risk, and many carry pathogenic variants that are well-managed. The intersection identifies the highest-priority cohort for care escalation.

---

## Anomaly Detection Definition

A patient record is flagged as anomalous if it meets **any** of the following criteria:

| Rule | Definition | Clinical Rationale |
|---|---|---|
| Extreme lab value | Any `test_value` beyond 3× IQR from median for that test type | Values this extreme are likely measurement errors or critical alerts |
| Impossible age | `date_of_birth` yields age < 0 or > 120 | Data entry error |
| Missing critical fields | `patient_id` or `date_of_birth` is null | Unidentifiable record — cannot be safely joined |
| Duplicate visit | Same `patient_id` + `test_name` + `collection_date` appears more than once | Likely duplicate submission |
| Orphan record | `patient_id` in labs/diagnoses/variants with no match in patients table | Referential integrity violation |

Anomalous records are flagged with `is_anomaly=True` and `anomaly_reason` columns — they are **not deleted**, preserving auditability.

---

## Setup and Installation

### Prerequisites

- Python 3.11+
- Docker Desktop (for containerized run)
- Git

### Local (venv)

```bash
git clone https://github.com/ANURAGNAKUL2702/CLOVERTEX-DATA-PIPELINE.git
cd CLOVERTEX-DATA-PIPELINE
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS/Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### Environment Variables

```bash
# Windows
setx GROQ_API_KEY "your_groq_api_key_here"

# macOS/Linux
export GROQ_API_KEY="your_groq_api_key_here"
```

> Get your Groq API key at [console.groq.com](https://console.groq.com). **Never commit your key to GitHub** — use environment variables or a `.env` file (add `.env` to `.gitignore`).
> The pipeline works without a Groq key — note category classification falls back to rule-based logic automatically.

### Place Input Files

Drop your source files into `data/raw_input/`. Supported formats: `.csv`, `.json`, `.parquet`, `.xlsx`, `.xls`

File naming conventions for automatic dataset routing:

| Name contains | Routed to |
|---|---|
| `patient` | patients |
| `lab` or `test` | labs |
| `diagnos` or `icd` | diagnoses |
| `variant` or `genomic` | variants |
| `medication` | medications |
| `note` | notes |

---

## Running the Pipeline

### Full Pipeline

```bash
python pipeline/main.py
```

### Partial Run (subset of stages)

```bash
python pipeline/main.py --start-at ingestion --stop-at analytics
python pipeline/main.py --start-at analytics
python pipeline/main.py --stop-at cleaning
```

Available stage names: `ingestion`, `cleaning`, `transformation`, `analytics`, `visualization`, `validation`, `manifest`

### Individual Stages

```bash
python pipeline/ingestion/ingest.py
python pipeline/cleaning/clean.py
python pipeline/transformation/transform.py
python pipeline/analytics/analyze.py
python pipeline/visualization/plots.py
python pipeline/validation/validate.py
python pipeline/validation/manifest.py
```

### Docker (Full Pipeline)

```bash
docker-compose up --build
```

Docker image: `clovertex-pipeline-pip:latest` (1.14 GB)

Outputs are written to the mounted `datalake/` volume and persist after the container exits.

---

## Outputs Reference

### Consumption Parquet Files

| File | Location | Description |
|---|---|---|
| `patients_unified_analytics.parquet` | `consumption/v1/unified/` | Full joined patient record |
| `patient_summary.parquet` | `consumption/v1/analytics/` | Demographics aggregation |
| `lab_statistics.parquet` | `consumption/v1/analytics/` | Per-test stats + trends |
| `diagnosis_frequency.parquet` | `consumption/v1/analytics/` | ICD-10 chapter frequencies |
| `variant_hotspots.parquet` | `consumption/v1/analytics/` | Top 5 pathogenic genes |
| `high_risk_patients.parquet` | `consumption/v1/analytics/` | High-risk patient cohort |

### Sample Unified Record

```
patient_id         | ALPHA-0001
site               | Alpha General
labs_count         | 5
latest_lab_test    | HbA1c
latest_lab_value   | 8.2
top_diagnosis_code | E11.9
top_medication     | Metformin
top_gene           | BRCA1
abnormal_lab_count | 2
high_risk_patient  | True
```

### Quality Report Structure

```json
{
  "generated": "2024-06-15 10:30:00",
  "ingestion": { ... },
  "cleaning": {
    "patients": { "nulls_handled": 15, "duplicates_removed": 20 },
    "labs":     { "nulls_handled": 42, "duplicates_removed": 3  }
  },
  "orphan_records": {
    "labs": 7, "diagnoses": 2, "variants": 0
  },
  "schema_mismatches": {
    "patients": [{ "only_in_base": ["blood_group"], "only_in_other": [] }]
  }
}
```

---

## Data Quality Reporting

The pipeline produces `logs/quality/data_quality_report.json` after each run containing:

- **Nulls handled** — count of null/empty/NA values standardized per dataset
- **Duplicates removed** — row-level duplicates dropped during cleaning
- **Orphan records** — records in child tables (labs, diagnoses, variants) with no matching `patient_id` in the patients table
- **Schema mismatches** — column-level differences detected between files within the same dataset (e.g., one site sends `blood_group`, another doesn't)

All anomalous records are flagged in-place — nothing is silently deleted.

---

## CI/CD

GitHub Actions runs on every push and pull request to `main`:

```yaml
Jobs:
  lint    → ruff check pipeline/
  build   → docker build .
```

View live CI runs → [github.com/ANURAGNAKUL2702/CLOVERTEX-DATA-PIPELINE/actions](https://github.com/ANURAGNAKUL2702/CLOVERTEX-DATA-PIPELINE/actions)

A failing lint or failed Docker build blocks the PR. The `main` branch is always in a deployable state.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `FileNotFoundError: data/raw_input` | Input directory missing | `mkdir -p data/raw_input` and add files |
| `Unknown dataset for file: xyz.csv` | Filename doesn't match any routing rule | Rename file to include `patient`, `lab`, `diagnos`, etc. |
| `Cannot reach api.groq.com` | No internet or invalid API key | Set `GROQ_API_KEY` or ignore (rule-based fallback activates automatically) |
| `No patients data for partition` | Cleaning ran but produced no unified patient file | Check `clean.py` logs — patient files may have failed loading |
| `⚠ No unified dataset found` | Transformation hasn't run yet | Run stages in order: ingest → clean → transform → analytics |
| Empty plots | Analytics stage hasn't run | Run `analyze.py` before `plots.py` |
| Docker exits non-zero | A pipeline stage failed | Check container logs: `docker-compose logs` |

---

## Future Improvements

- Unit tests with `pytest` and data contracts with `pandera`
- Incremental processing using watermark-based change detection
- Replace pandas with DuckDB or Polars for 10–100x performance at scale
- Stream ingestion via Kafka for real-time lab result processing
- Great Expectations integration for automated data quality gates
- Monitoring dashboard (Grafana + Prometheus) for pipeline health metrics
- Role-based access control for PHI-sensitive columns
