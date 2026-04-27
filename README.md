# Clovertex Healthcare Data Pipeline

A production-style healthcare data engineering pipeline that ingests multi-source clinical datasets (CSV, JSON, Parquet), standardizes them into a data lake (raw → refined → consumption), and produces analytics, visualizations, and unified patient-level outputs.

## Architecture Diagram

If you want a visual diagram, this is the logical flow:

Raw Inputs → Ingestion → Raw Layer → Cleaning → Refined Layer → Transformation → Consumption Layer → Analytics + Visualization + Validation

### Technical Architecture (Depth)

- **Processing**: Python batch jobs (pandas) executed per stage with clear boundaries and logging.
- **Storage**: Parquet in a data lake with `ingest_date` partitioning.
- **Orchestration**: `pipeline/main.py` runs stages in order with optional start/stop flags.
- **Metadata**: `manifest.json` per lake layer + quality reports in `logs/quality/`.

## Folder Structure

```
data/
  raw_input/
datalake/
  raw/
  refined/v1/
  consumption/v1/
  consumption/plots/
logs/
  ingestion/
  quality/
pipeline/
  ingestion/
  cleaning/
  transformation/
  analytics/
  visualization/
  validation/
```

## Pipeline Stages

1. **Ingestion**
   - Reads CSV/JSON/Parquet from `data/raw_input/`
   - Normalizes column names and patient keys
   - Adds metadata (`_source_file`, `_ingestion_time`, `_dataset`, `_batch_id`)
   - Writes Parquet to `datalake/raw/<dataset>/ingest_date=YYYY-MM-DD/`
   - Idempotent: skips files already written

2. **Cleaning**
   - JSON flattening and schema cleanup
   - Standardizes nulls, dates, and categorical values
   - Removes duplicates
   - Produces unified patients table
   - Writes to `datalake/refined/v1/`

3. **Transformation**
   - Builds a unified, analytics-ready patient dataset
   - Joins patient data with labs, diagnoses, medications, and genomics
   - Filters genomics to pathogenic / likely pathogenic
   - Detects abnormal labs and high-risk patients
   - Writes to `datalake/consumption/v1/`

4. **Analytics**
   - Generates JSON reports per dataset
   - ICD-10 chapter mapping, lab outliers, site and age distributions
   - High-risk patient metrics from unified dataset

5. **Visualization**
   - Generates PNG plots using matplotlib
   - Saves to `datalake/consumption/plots/`

6. **Validation**
   - Produces `data_quality_report.json`
   - Aggregates nulls handled, duplicates removed, orphan records, schema mismatches

## Dataset Descriptions

- **patients**: demographics, site, admission/discharge, contact info
- **labs**: test name/value, collection date, ordering physician
- **diagnoses**: ICD-10 codes, severity, status, primary flag
- **medications**: medication name, dosage, route, status, dates
- **notes**: note category, author, word count, note metadata
- **variants**: gene, variant type, allele frequency, clinical significance

## Data Flow

Raw Input → `datalake/raw` → `datalake/refined/v1` → `datalake/consumption/v1` → reports + plots

## Key Features

- Idempotent ingestion (skip existing files)
- JSON flattening + schema normalization
- Date/boolean standardization
- Patient-level unified dataset
- Genomics filtering (pathogenic / likely pathogenic)
- High-risk patient detection
- Data quality reporting
- Metadata tracking with `manifest.json`

## Bonus Feature (LLM Classification)

Clinical note categories are standardized using Groq LLM with fallback rules:

- Input: `note_category`
- Output: `standard_category`
- Categories: `admission`, `discharge`, `lab_review`, `nursing`, `consultation`, `progress`, `procedure`, `other`

## Setup Instructions

### Local (venv)

```bash
python -m venv .venv
.
venv\Scripts\activate
pip install -r requirements.txt
```

### Docker

```bash
docker-compose up --build
```

### Environment Variables

```bash
setx GROQ_API_KEY "YOUR_GROQ_KEY"
```

## How to Run

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

### Full Pipeline

```bash
python pipeline/main.py
```

You can also run a subset:

```bash
python pipeline/main.py --start-at ingestion --stop-at visualization
```

## Sample Outputs

- `datalake/consumption/v1/unified/.../patients_unified_analytics.parquet`
- `datalake/reports/analytics/*.json`
- `datalake/consumption/plots/*.png`

Example unified output (sample columns):

```
patient_id | site | labs_count | latest_lab_test | top_diagnosis_code | top_medication | top_gene | abnormal_lab_count | high_risk_patient
ALPHA-0001 | Alpha General | 5 | HbA1c | E11.9 | Metformin | BRCA1 | 1 | True
```

## Data Quality Strategy

- Nulls handled and duplicates removed during cleaning
- Orphan record detection during validation
- Schema mismatches detected across datasets
- Report: `logs/quality/data_quality_report.json`

## Refined Partitioning Strategy

- Default: `datalake/refined/v1/<dataset>/ingest_date=YYYY-MM-DD/`
- Labs: `datalake/refined/v1/labs/test_name=<lab_test>/ingest_date=YYYY-MM-DD/`
   - `test_name` is normalized to lowercase with underscores

## Design Decisions

- **Partitioning by ingest_date** for reproducibility and batch tracking
- **Unified patient dataset** to enable analytics-ready joins
- **Idempotent ingestion** to prevent duplicate writes
- **Structured logging + manifests** for auditability

## Future Improvements

- Add unit tests and data contracts
- Add incremental processing for large datasets
- Extend LLM labeling to other domains
- Add monitoring dashboards

## CI/CD

GitHub Actions workflow runs on every push/PR to `main`:

- `ruff check pipeline` (lint)
- `docker build .` (container build validation)

## Business Value

- Improves clinical reporting readiness by unifying patient data across sources
- Enables rapid analytics with curated consumption datasets
- Flags high-risk patients for care prioritization
- Ensures auditability via manifests, logs, and quality reporting

## Conclusion

This project demonstrates a full production-style healthcare pipeline with data lake architecture, quality reporting, LLM enrichment, and DevOps automation. It is structured for real-world deployment and recruiter review.
