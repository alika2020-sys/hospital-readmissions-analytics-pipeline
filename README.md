# 🏥 Hospital Readmissions Analytics Pipeline
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Tableau](https://img.shields.io/badge/Tableau-E97627?style=for-the-badge&logo=tableau&logoColor=white)
![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)
![DBeaver](https://img.shields.io/badge/DBeaver-382923?style=for-the-badge&logo=dbeaver&logoColor=white)
![GitHub](https://img.shields.io/badge/GitHub-100000?style=for-the-badge&logo=github&logoColor=white)

An end-to-end data analytics and data warehouse engineering project analyzing U.S. hospital readmission rates using CMS (Centers for Medicare & Medicaid Services) data. This project mirrors the core responsibilities of a Data Warehouse Engineer supporting federal healthcare programs — including data cleaning, star schema design, advanced SQL analysis, performance optimization, compliance considerations, and AI/ML readiness.

This project was designed to demonstrate applied expertise in data warehouse systems, database performance optimization, data lineage, compliance, and AI readiness — skills directly applicable to improving outcomes for Veterans and other federal healthcare populations.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Skills Demonstrated](#skills-demonstrated)
- [Data Sources](#data-sources)
- [Tech Stack](#tech-stack)
- [Project Architecture](#project-architecture)
- [Database Design — Star Schema](#database-design--star-schema)
- [Data Cleaning](#data-cleaning)
- [Descriptive Statistics](#descriptive-statistics)
- [Data Lineage](#data-lineage)
- [Advanced SQL Analysis](#advanced-sql-analysis)
- [Data Quality Checks](#data-quality-checks)
- [Performance Optimization](#performance-optimization)
- [Data Privacy, Security & Compliance](#data-privacy-security--compliance)
- [Tableau Dashboard](#tableau-dashboard)
- [Key Findings](#key-findings)
- [AI Readiness](#ai-readiness)
- [Python ETL Pipeline](#python-etl-pipeline)
- [Cross-Database Compatibility](#cross-database-compatibility)
- [Integrating Disparate Data Sources](#integrating-disparate-data-sources)
- [How to Run This Project](#how-to-run-this-project)
- [Future Improvements](#future-improvements)

---

## Project Overview

Hospital readmissions — when a patient is discharged and returns within 30 days — are a key indicator of healthcare quality and a major cost driver for the U.S. healthcare system. The CMS Hospital Readmissions Reduction Program (HRRP) penalizes hospitals with excess readmission rates above the national expected rate. The same readmission challenges that affect the broader U.S. healthcare system are directly relevant to VA medical centers and Veterans health programs, where chronic conditions like Heart Failure, COPD, and Pneumonia drive significant readmission rates among Veteran populations.

This project analyzes FY2026 HRRP data to:
- Identify hospitals and states with the highest excess readmission ratios
- Detect SLA violations where hospitals significantly exceed expected readmission thresholds
- Surface bottleneck facilities that underperform across multiple measures
- Build a scalable, production-ready star schema data warehouse for ongoing reporting
- Establish data lineage, quality checks, and compliance standards across the full analytics lifecycle
- Visualize findings through an interactive Tableau dashboard
- Structure the data to support future AI/ML and NLP implementations

---

## Skills Demonstrated

This table maps each project component directly to the core responsibilities and required qualifications of a Data Warehouse Engineer supporting federal healthcare programs:

| JD Requirement | How This Project Demonstrates It |
|---------------|----------------------------------|
| Query, wrangle, and join data from analytical and transactional databases | Multi-table star schema with complex JOINs, CTEs, window functions, subqueries |
| Descriptive statistics and summary data analysis | Dedicated descriptive statistics section with mean, median, stddev, percentiles |
| Data lineage, quality checks, and compliance standards | Explicit data lineage documentation, 6 data quality checks, HIPAA compliance section |
| Evaluate and optimize data warehouse architecture | Star schema design with documented architectural decisions and trade-offs |
| Identify and resolve bottlenecks | Bottleneck detection query using window functions and deviation analysis |
| Indexing and partitioning strategies | Full indexing and partitioning strategy with documented rationale |
| Advise on data models to prepare for AI/ML including NLP | Dedicated AI readiness section covering classification, clustering, and NLP use cases |
| Integrate disparate data sources into existing ecosystems | Section on integrating CMS, patient-level, and external data sources |
| Data privacy, security, and compliance | HIPAA-aligned compliance section with role-based access and anonymization guidance |
| Proficiency with Tableau and Power BI | Tableau dashboard with Power BI migration notes |
| Python proficiency and clean, commented code | Python ETL pipeline with documented, commented scripts |
| Strong documentation habits | All SQL fully commented, README covers full methodology end to end |
| Communicate technical details to non-technical audiences | Key Findings section written for non-technical stakeholders |

---

## Data Sources

| File | Description | Rows |
|------|-------------|------|
| `FY_2026_Hospital_Readmissions_Reduction_Program_Hospital.csv` | CMS official hospital readmissions data for FY2026 | 18,330 |
| `train_df.csv` | Patient-level readmission training dataset | 5,000 |
| `test_df.csv` | Patient-level readmission test dataset | 2,000 |
| `sample_submission.csv` | Submission format for patient-level predictions | 2,000 |

**CMS Data Columns:**
- `Facility Name`, `Facility ID`, `State`
- `Measure Name` — 6 readmission measures (AMI, CABG, COPD, Heart Failure, Hip/Knee, Pneumonia)
- `Number of Discharges`, `Number of Readmissions`
- `Excess Readmission Ratio`, `Predicted Readmission Rate`, `Expected Readmission Rate`
- `Start Date`, `End Date`, `Footnote`

**Patient-Level Data Columns:**
- `age`, `gender`, `primary_diagnosis`, `num_procedures`
- `days_in_hospital`, `comorbidity_score`, `discharge_to`, `readmitted`

---

## Tech Stack

**Implemented in this project:**
- **Database:** PostgreSQL
- **SQL Client:** DBeaver
- **Visualization:** Tableau
- **Version Control:** Git / GitHub
- **Data Format:** CSV

**Enterprise / Production Equivalents:**
- **Databases:** Oracle, MS SQL Server (syntax notes included in Cross-Database Compatibility section)
- **BI Tools:** Power BI (migration notes included in Tableau Dashboard section)
- **Cloud Platforms:** AWS Redshift, Azure Synapse, Google BigQuery (architecture is cloud-portable)
- **Big Data:** Apache Spark (partitioning strategy designed to scale to Spark-based pipelines)
- **ETL:** Python (pandas, psycopg2) — see Python ETL Pipeline section

---

## Project Architecture

```
Raw CSV Files (CMS + Patient Level)
            │
            ▼
    Data Ingestion Layer
    (PostgreSQL via DBeaver)
            │
            ├── Raw Tables
            │       ├── fy (CMS hospital data)
            │       ├── train_df (patient training data)
            │       ├── test_df (patient test data)
            │       └── sample_submission
            │
            ├── Cleaning Layer
            │       └── fy_clean
            │               ├── Null handling
            │               ├── Type casting
            │               ├── Date conversion
            │               └── Footnote decoding
            │
            ├── Data Warehouse Layer (Star Schema)
            │       ├── dim_facility
            │       ├── dim_measure
            │       ├── dim_date
            │       └── fact_readmissions
            │
            ├── Quality & Lineage Layer
            │       ├── 6 Data Quality Checks
            │       └── Data Lineage Documentation
            │
            ├── Analytics Layer
            │       ├── Descriptive Statistics
            │       ├── Bottleneck Detection
            │       ├── SLA Violations
            │       ├── State Performance Analysis
            │       ├── Performance Tiers
            │       └── Top Worst Performers
            │
            └── Presentation Layer
                    ├── Tableau Dashboard
                    └── Key Findings (Stakeholder Summary)
```

---

## Database Design — Star Schema

A star schema was designed to optimize query performance and simplify reporting in Tableau. The fact table sits at the center with three dimension tables branching outward.

```
           dim_facility
           ─────────────
           facility_id (PK)
           facility_name
           state
                │
                │
dim_measure ────┼──── fact_readmissions ──── dim_date
─────────────   │     ─────────────────      ─────────────
measure_id (PK) │     fact_id (PK)           date_id (PK)
measure_code    │     facility_id (FK)        start_date
measure_name    │     measure_id (FK)         end_date
                │     date_id (FK)            start_year
                       number_of_discharges   start_month
                       number_of_readmissions end_year
                       excess_readmission_ratio end_month
                       predicted_readmission_rate
                       expected_readmission_rate
                       footnote_description
```

**Architectural decisions and trade-offs:**

A star schema was chosen over a snowflake schema because the dimension tables in this dataset are small and stable — there is no need for further normalization of `dim_facility` or `dim_measure`. The performance benefit of fewer JOINs in a star schema outweighs the minor storage savings of a snowflake design, especially when the primary consumers are BI tools like Tableau that benefit from simpler relationship models. In a production VA or federal healthcare environment, this schema would sit inside a larger enterprise data warehouse alongside clinical, financial, and administrative data marts.

**Why a star schema?**
- Faster query performance through denormalization
- Simpler JOIN logic — always join through the fact table
- Directly compatible with Tableau and Power BI relationship models
- Industry standard for data warehouses and BI reporting
- Designed to scale — additional dimension tables (e.g., `dim_geography`, `dim_patient`) can be added without restructuring the fact table

---

## Data Cleaning

Before analysis, the raw CMS data required several cleaning steps. A working copy (`fy_clean`) was created before any modifications so the original raw table remains intact and auditable — a best practice in enterprise data environments.

**Issues identified:**
- `Number of Discharges` — 10,088 rows (55%) containing `"N/A"` instead of numeric values
- `Number of Readmissions` — 6,610 rows with `"N/A"` and `"Too Few to Report"`
- `Excess Readmission Ratio`, `Predicted/Expected Readmission Rate` — 6,610 rows each with `"N/A"`
- `Footnote` — mostly blank with codes (1, 5, 7, 29) requiring decoding into human-readable labels
- `Start Date` / `End Date` — stored as text strings (`MM/DD/YYYY`) instead of proper DATE type

**Cleaning steps applied:**
1. Created `fy_clean` as a non-destructive working copy of the raw table
2. Replaced all `"N/A"` and `"Too Few to Report"` values with `NULL` to enable proper numeric aggregation
3. Cast text columns to proper numeric types (`INTEGER`, `NUMERIC`) to enforce data type integrity
4. Converted date strings to PostgreSQL `DATE` type to enable date arithmetic and range filtering
5. Added `footnote_description` column decoding CMS footnote codes into human-readable audit labels

See full cleaning SQL in `/sql/cleaning/clean_fy_data.sql`

---

## Descriptive Statistics

Summary statistics were produced to understand the distribution of key metrics before any deeper analysis. This step is critical in healthcare data environments to identify skewness, outliers, and data quality issues that could distort downstream analysis.

```sql
-- Descriptive statistics for key numeric measures
SELECT
    -- Central tendency
    ROUND(AVG(excess_readmission_ratio), 4)                                    AS mean_excess_ratio,
    ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP
        (ORDER BY excess_readmission_ratio)::NUMERIC, 4)                       AS median_excess_ratio,

    -- Spread
    ROUND(STDDEV(excess_readmission_ratio), 4)                                 AS stddev_excess_ratio,
    ROUND(MIN(excess_readmission_ratio), 4)                                    AS min_excess_ratio,
    ROUND(MAX(excess_readmission_ratio), 4)                                    AS max_excess_ratio,

    -- Interquartile range
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
        (ORDER BY excess_readmission_ratio)::NUMERIC, 4)                       AS q1_excess_ratio,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
        (ORDER BY excess_readmission_ratio)::NUMERIC, 4)                       AS q3_excess_ratio,

    -- Volume metrics
    ROUND(AVG(number_of_discharges), 1)                                        AS avg_discharges,
    ROUND(AVG(number_of_readmissions), 1)                                      AS avg_readmissions,
    ROUND(AVG(predicted_readmission_rate), 4)                                  AS avg_predicted_rate,

    -- Completeness
    COUNT(*)                                                                   AS total_records,
    COUNT(excess_readmission_ratio)                                            AS non_null_ratio_records,
    ROUND(100.0 * COUNT(excess_readmission_ratio) / COUNT(*), 2)              AS pct_complete

FROM fact_readmissions;
```

See `/sql/analysis/descriptive_statistics.sql`

---

## Data Lineage

Data lineage documents how data moves, transforms, and is consumed at each stage of the pipeline. In federal healthcare environments, lineage is a compliance and audit requirement — it ensures that every number in a report can be traced back to its source.

```
SOURCE
  └── CMS HRRP FY2026 Public Dataset (csv)
        └── Ingested as raw table: fy
              └── Cleaned copy: fy_clean
                    ├── Transformations applied:
                    │     ├── NULL substitution for N/A values
                    │     ├── Type casting (VARCHAR → INTEGER, NUMERIC, DATE)
                    │     └── Footnote decoding (codes → descriptions)
                    │
                    └── Star Schema (data warehouse layer)
                          ├── dim_facility    ← derived from fy_clean (DISTINCT facility records)
                          ├── dim_measure     ← manually seeded from CMS measure definitions
                          ├── dim_date        ← derived from fy_clean (DISTINCT date ranges)
                          └── fact_readmissions ← joined from fy_clean + all dimension tables
                                │
                                └── Analytics Layer
                                      ├── Descriptive statistics
                                      ├── Bottleneck detection
                                      ├── SLA violation reports
                                      └── Tableau Dashboard (presentation layer)
```

**Lineage principles applied:**
- Raw tables are never modified — all transformations produce new tables or columns
- Every transformation is documented in SQL with inline comments explaining the reason for the change
- The cleaning step is a discrete, auditable layer between raw ingestion and warehouse loading
- Dimension tables record their derivation source so downstream consumers know where values originated

In a production Oracle or MS SQL Server environment, lineage would be tracked using a dedicated metadata table or a lineage tool such as Apache Atlas or Microsoft Purview.

---

## Advanced SQL Analysis

All SQL scripts are fully commented. See `/sql/analysis/` for complete files.

### Bottleneck Detection (Window Functions)
Identifies hospitals that consistently underperform their state average across multiple readmission measures. Uses window functions to calculate deviation from state averages and ranks facilities within each state. Bottleneck facilities are flagged as Critical, High Risk, Moderate Risk, or On Track.

See `/sql/analysis/bottleneck_detection.sql`

### SLA Violation Analysis
Defines three SLA thresholds based on CMS benchmarks and flags hospitals breaching them:
- Excess Readmission Ratio > 1.10 (10% worse than expected)
- Predicted Readmission Rate > 15%
- Number of Discharges < 25 (volume too low to benchmark reliably)

Calculates a violation rate percentage per hospital and assigns a status from SLA Compliant to Critical SLA Breach. Rankings are produced nationally using window functions.

See `/sql/analysis/sla_violations.sql`

### State Performance Analysis (CTEs)
Uses Common Table Expressions to compare each state's average excess readmission ratio against the national average. Labels states as Above Average or Below Average and ranks them nationally. CTEs were chosen over subqueries here for readability and to make the logic auditable step by step.

See `/sql/analysis/state_performance.sql`

### Hospital Performance Tiers (CASE WHEN)
Categorizes every hospital-measure combination into performance tiers: Excellent, Good, Needs Improvement, or Poor. Thresholds are based on CMS benchmark guidance and are documented inline.

See `/sql/analysis/performance_tiers.sql`

### Top 10% Worst Performers (Subqueries)
Uses `PERCENTILE_CONT` to identify hospitals in the top 10% worst performers nationally. This query is designed to support priority intervention lists — identifying which facilities should be flagged for review first.

See `/sql/analysis/top_worst_performers.sql`

---

## Data Quality Checks

Six data quality checks were implemented before any analysis was run. Running quality checks before analysis is a non-negotiable standard in federal healthcare data environments where decisions impact patient care and program funding.

| Check | Purpose | Risk if Skipped |
|-------|---------|-----------------|
| Null/Missing Value Check | Count and percentage of missing values per column | Analysis on incomplete data produces misleading averages |
| Duplicate Check | Identify duplicate facility-measure-date combinations | Duplicate rows inflate aggregate counts and ratios |
| Outlier Detection | Flag values more than 3 standard deviations from the mean | Outliers skew averages and can trigger false SLA alerts |
| Referential Integrity Check | Verify all foreign keys resolve to valid dimension records | Orphaned fact records are silently excluded from JOIN-based reports |
| Range Validity Check | Confirm numeric values fall within realistic boundaries | Out-of-range values indicate data entry errors or ingestion failures |
| Completeness by State | Identify which states have the highest rates of missing data | State-level reports appear accurate but are based on partial data |

See full scripts in `/sql/data_quality/`

---

## Performance Optimization

### Indexing Strategy

Indexes allow the database engine to locate rows without scanning the entire table, dramatically improving query performance on large datasets.

**Primary Key Indexes** are created automatically by PostgreSQL on all primary key columns (`fact_id`, `facility_id`, `measure_id`, `date_id`), making JOIN operations between the fact and dimension tables fast by default. In Oracle and MS SQL Server, clustered indexes serve a similar role and are created on primary keys by default.

**Foreign Key Indexes** are NOT created automatically by PostgreSQL and must be added manually. Since the fact table is constantly queried by joining on foreign key columns, indexing them significantly reduces query time:

```sql
-- Index foreign keys in the fact table for faster JOINs
CREATE INDEX idx_fact_facility ON fact_readmissions(facility_id);
CREATE INDEX idx_fact_measure  ON fact_readmissions(measure_id);
CREATE INDEX idx_fact_date     ON fact_readmissions(date_id);
```

**Selective Column Indexes** are added to columns heavily used in WHERE and ORDER BY clauses:

```sql
-- Frequently filtered and sorted columns
CREATE INDEX idx_excess_ratio   ON fact_readmissions(excess_readmission_ratio);
CREATE INDEX idx_facility_state ON dim_facility(state);
```

`excess_readmission_ratio` is indexed because nearly every analytical query filters or sorts by it — the SLA violation checks, bottleneck detection, and outlier analysis all reference it heavily. `state` is indexed on the dimension table because geographic filtering is a core use case across the dashboard and all state-level reports.

**Why not index everything?** Indexes consume disk space and slow down write operations (INSERT, UPDATE, DELETE) because the index must be updated with every data change. The goal is to index columns that are read frequently but written to infrequently — which describes all the columns above in this project.

---

### Partitioning Strategy

Partitioning splits a large table into smaller physical pieces while maintaining one logical table. Although the current dataset does not require partitioning, designing with it in mind is essential for production healthcare systems where millions of records are added annually — for example a VA data warehouse ingesting encounter data across 170+ VA medical centers.

**Range Partitioning by Fiscal Year** is the most natural strategy for this dataset. When a query filters by date range, PostgreSQL uses partition pruning to skip irrelevant partitions entirely — instead of scanning all records, it only scans the relevant fiscal year partition:

```sql
-- Partitioned fact table by fiscal year
CREATE TABLE fact_readmissions_partitioned (
    fact_id                    SERIAL,
    facility_id                VARCHAR(10),
    measure_id                 INTEGER,
    date_id                    INTEGER,
    number_of_discharges       INTEGER,
    number_of_readmissions     INTEGER,
    excess_readmission_ratio   NUMERIC(10,4),
    predicted_readmission_rate NUMERIC(10,4),
    expected_readmission_rate  NUMERIC(10,4),
    start_date                 DATE
) PARTITION BY RANGE (start_date);

-- One partition per CMS fiscal year
CREATE TABLE fact_readmissions_fy2022
    PARTITION OF fact_readmissions_partitioned
    FOR VALUES FROM ('2021-07-01') TO ('2022-06-30');

CREATE TABLE fact_readmissions_fy2023
    PARTITION OF fact_readmissions_partitioned
    FOR VALUES FROM ('2022-07-01') TO ('2023-06-30');

CREATE TABLE fact_readmissions_fy2024
    PARTITION OF fact_readmissions_partitioned
    FOR VALUES FROM ('2023-07-01') TO ('2024-06-30');
```

As CMS releases new fiscal year data annually, new partitions can be added without restructuring the existing table — a critical advantage in long-running federal programs.

**List Partitioning by Region** would benefit regional health network reporting where queries are almost always filtered by state:

```sql
-- Regional partitions for geographically scoped reporting
CREATE TABLE fact_readmissions_south
    PARTITION OF fact_readmissions_partitioned
    FOR VALUES IN ('AL', 'GA', 'FL', 'MS', 'SC', 'TN');

CREATE TABLE fact_readmissions_northeast
    PARTITION OF fact_readmissions_partitioned
    FOR VALUES IN ('NY', 'NJ', 'CT', 'MA', 'PA', 'VT');
```

**Partitioning vs Indexing:** Indexing is best when you need to find specific rows quickly within a table. Partitioning is best when you need to eliminate entire sections of a table from a scan altogether. In a production system both are used together — partitioning by fiscal year for time-based queries, with indexes on foreign keys and frequently filtered columns within each partition.

---

## Data Privacy, Security & Compliance

Healthcare data — particularly data tied to federal programs — requires strict adherence to privacy, security, and compliance standards. The following principles were applied in this project and would be enforced in a production federal healthcare environment.

### HIPAA Alignment

The CMS HRRP dataset used in this project is a publicly released, de-identified aggregate dataset and does not contain Protected Health Information (PHI). However, the patient-level `train_df` dataset contains quasi-identifiers (age, gender, diagnosis, discharge destination) that in a real clinical environment would be subject to HIPAA Privacy and Security Rules.

In a production environment the following controls would be applied:

**De-identification:** Patient records would be de-identified using HIPAA Safe Harbor or Expert Determination methods before being loaded into the analytical database. Direct identifiers (name, SSN, MRN, date of birth) would be removed or generalized.

**Role-Based Access Control (RBAC):** Database roles would be created to enforce least-privilege access — analysts would have read-only access to de-identified views, not the underlying tables:

```sql
-- Example: Create read-only analyst role
CREATE ROLE analyst_readonly;
GRANT SELECT ON fact_readmissions TO analyst_readonly;
GRANT SELECT ON dim_facility TO analyst_readonly;
GRANT SELECT ON dim_measure TO analyst_readonly;
GRANT SELECT ON dim_date TO analyst_readonly;

-- Deny access to raw patient-level tables
REVOKE ALL ON train_df FROM analyst_readonly;
```

**Audit Logging:** All queries against sensitive tables would be logged for compliance auditing. PostgreSQL supports this via `pgaudit`. Oracle and MS SQL Server have native audit trail capabilities built in.

**Data Masking:** In non-production environments (dev, test), sensitive columns would be masked:

```sql
-- Example: Masked view for development environments
CREATE VIEW train_df_masked AS
SELECT
    -- Age generalized into buckets instead of exact value
    CASE
        WHEN age < 30 THEN 'Under 30'
        WHEN age BETWEEN 30 AND 49 THEN '30-49'
        WHEN age BETWEEN 50 AND 69 THEN '50-69'
        ELSE '70+'
    END                    AS age_group,
    gender,
    primary_diagnosis,
    num_procedures,
    days_in_hospital,
    comorbidity_score,
    discharge_to,
    readmitted
FROM train_df;
```

**Encryption:** In a production deployment, data at rest would be encrypted using Transparent Data Encryption (TDE) available in Oracle, MS SQL Server, and PostgreSQL. Data in transit would be encrypted via SSL/TLS.

### FedRAMP and VA-Specific Considerations

Federal systems storing or processing healthcare data for Veterans are subject to additional frameworks beyond HIPAA, including FedRAMP (for cloud deployments), FISMA, and VA Handbook 6500. Any cloud deployment of this pipeline (e.g., AWS GovCloud, Azure Government) would require FedRAMP authorization and adherence to VA data residency requirements ensuring Veterans' data does not leave approved federal environments.

---

## Tableau Dashboard

🔗 **[View Live Dashboard on Tableau Public](https://public.tableau.com/app/profile/san.a7458/viz/U_S_HospitalReadmissionsAnalyticsDashboard/Dashboard1)**

The Tableau dashboard connects directly to PostgreSQL via the star schema and includes the following views:

| Visual | Description |
|--------|-------------|
| KPI Cards | Total hospitals, national avg excess ratio, total readmissions, % SLA violations |
| US Choropleth Map | Excess readmission ratio by state, color coded green to red |
| Top 10 Worst Hospitals | Horizontal bar chart ranked by excess ratio |
| Performance by Measure | Average excess ratio across all 6 CMS measure types |
| Scatter Plot | Predicted vs Expected readmission rate per hospital |
| Heatmap | State vs Measure performance matrix |
| Histogram | Distribution of excess readmission ratios with SLA threshold line |

**Connecting Tableau to PostgreSQL:**
1. Open Tableau → Connect → PostgreSQL
2. Enter server (`localhost`), port (`5432`), database name, username, password
3. Drag `fact_readmissions` as the primary table
4. Join `dim_facility`, `dim_measure`, and `dim_date`
5. Tableau will respect the star schema relationships automatically

**Power BI Migration Note:** This dashboard can be replicated in Power BI with minimal changes. The star schema connects directly to Power BI via the PostgreSQL connector. The fact-dimension relationships map directly to Power BI's model view. DAX measures would replace Tableau calculated fields for KPI cards and ratio calculations.

<img width="1351" height="745" alt="image" src="https://github.com/user-attachments/assets/1bfb943a-6d68-4073-835a-ca9f4dd227fd" />

---

## Key Findings

The following findings are written for a non-technical stakeholder audience — translating analytical outputs into plain-language insights that drive decision-making:

- Hospitals flagged with CMS footnote code 29 ("Too few discharges") represent a significant share of missing data. These are predominantly small, rural facilities that do not meet the minimum volume threshold for reliable benchmarking. This has implications for rural healthcare policy — these hospitals are invisible in national benchmarks despite potentially serving high-risk populations.
- Heart Failure and COPD measures consistently show higher excess readmission ratios nationally compared to surgical measures like Hip/Knee replacement. This suggests that chronic condition management — not procedural care — is the primary driver of avoidable readmissions, which has direct policy implications for care coordination and post-discharge support programs.
- Several states show greater than 50% missing data for key metrics, suggesting regional CMS reporting compliance gaps. Reports generated for these states should carry data completeness caveats to prevent misleading conclusions.
- A subset of hospitals appear as bottlenecks across multiple measures simultaneously, indicating systemic care quality issues rather than measure-specific problems. These facilities should be prioritized for targeted intervention over facilities that underperform on only one measure.

---

## AI Readiness

This dataset is structured to support future **machine learning and AI** use cases such as predicting **30-day patient readmission risk using supervised classification models (Logistic Regression, XGBoost, Random Forest), identifying hospitals at risk of CMS financial penalties before the fiscal year closes, clustering facilities into peer groups for fairer state-level benchmarking, and building anomaly detection pipelines to flag sudden readmission spikes in near real time.**

The star schema and cleaned fact tables are intentionally designed to serve as a **feature store**, making it straightforward to extract training-ready datasets with a single SELECT statement.

### Potential ML Use Cases

| Use Case | Model Type | Target Variable |
|----------|-----------|-----------------|
| Predict patient readmission risk | Classification (XGBoost, Random Forest) | `readmitted` (0/1) |
| Flag hospitals at CMS penalty risk | Classification (Logistic Regression) | SLA violation status |
| Cluster hospitals into peer groups | Unsupervised (K-Means) | Excess readmission ratio |
| Detect anomalous readmission spikes | Anomaly Detection (Isolation Forest) | Excess ratio deviation |
| Forecast next fiscal year readmission rates | Time Series (ARIMA, Prophet) | Predicted readmission rate |

### Natural Language Processing (NLP) Use Cases

The data warehouse architecture is also designed to support future NLP implementations, which are increasingly important in federal healthcare environments for processing unstructured clinical text:

| NLP Use Case | Description | Relevant Data |
|-------------|-------------|---------------|
| Clinical notes classification | Classify discharge summaries by readmission risk level | Patient-level discharge data |
| Measure name entity extraction | Extract condition types and procedure names from CMS text fields | `Measure Name` column |
| Automated report generation | Generate plain-language summaries of hospital performance for non-technical stakeholders | All fact and dimension tables |
| Chatbot / query interface | Allow stakeholders to query the data warehouse in natural language | Full star schema |

NLP pipelines would be built using Hugging Face Transformers with pre-trained clinical language models such as BioBERT or ClinicalBERT, fine-tuned on VA-specific clinical text. PyTorch would serve as the underlying deep learning framework.

### Why This Schema Supports AI

The star schema design directly supports ML workflows in several ways. The `fact_readmissions` table acts as a pre-joined feature table — a data scientist can query it with a single SELECT and immediately have a structured, analysis-ready dataset. The dimension tables provide clean categorical features (state, measure type) ready for one-hot or label encoding. The separation of raw data (`fy`) from cleaned data (`fy_clean`) ensures model training always uses validated, type-cast inputs. The `footnote_description` column provides interpretable audit context that can be used as an exclusion filter during model training to prevent the model from learning on unreliable low-volume data.

### Patient-Level Prediction Ready

The `train_df` and `test_df` tables contain patient-level features — age, gender, primary diagnosis, number of procedures, days in hospital, comorbidity score, and discharge destination — already structured for binary classification. A model trained on `train_df` predicting the `readmitted` column can be evaluated on `test_df` with predictions submitted in `sample_submission` format, making this project immediately extensible into a full supervised ML pipeline.

---

## Python ETL Pipeline

While the current implementation uses PostgreSQL and DBeaver for data ingestion and transformation, a production environment would automate this using a Python ETL pipeline. The pipeline below demonstrates how the full ingestion, cleaning, and loading workflow would be implemented programmatically with clean, commented, well-structured code.

```python
# etl_pipeline.py
# Purpose: Ingest CMS HRRP CSV data, apply cleaning transformations,
#          and load into PostgreSQL star schema tables
# Author: San
# Dependencies: pandas, psycopg2, sqlalchemy

import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# ----------------------------
# Configuration
# ----------------------------
DB_CONFIG = {
    "host":     "localhost",
    "port":     5432,
    "database": "your_database",
    "user":     "your_username",
    "password": "your_password"
}

CMS_FILE    = "FY_2026_Hospital_Readmissions_Reduction_Program_Hospital.csv"
TRAIN_FILE  = "train_df.csv"
TEST_FILE   = "test_df.csv"

# ----------------------------
# Step 1: Load raw data
# ----------------------------
def load_raw_data(filepath):
    """Load CSV file into a pandas DataFrame."""
    df = pd.read_csv(filepath)
    print(f"Loaded {len(df)} rows from {filepath}")
    return df

# ----------------------------
# Step 2: Clean CMS data
# ----------------------------
def clean_cms_data(df):
    """
    Apply cleaning transformations to the raw CMS HRRP dataset.
    - Replace N/A and 'Too Few to Report' with NaN
    - Cast numeric columns to proper types
    - Convert date strings to datetime
    - Decode footnote codes
    """
    # Replace non-numeric placeholders with NaN
    na_values = ["N/A", "Too Few to Report"]
    numeric_cols = [
        "Number of Discharges",
        "Number of Readmissions",
        "Excess Readmission Ratio",
        "Predicted Readmission Rate",
        "Expected Readmission Rate"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col].replace(na_values, pd.NA), errors="coerce")

    # Convert date strings to datetime
    df["Start Date"] = pd.to_datetime(df["Start Date"], format="%m/%d/%Y")
    df["End Date"]   = pd.to_datetime(df["End Date"],   format="%m/%d/%Y")

    # Decode footnote codes into human-readable descriptions
    footnote_map = {
        "1":  "No data available",
        "5":  "Results based on a shorter time period than required",
        "7":  "No cases meet the criteria for this measure",
        "29": "Too few discharges to reliably report"
    }
    df["footnote_description"] = df["Footnote"].map(footnote_map)

    print(f"Cleaning complete. {df.isnull().sum().sum()} null values in cleaned dataset.")
    return df

# ----------------------------
# Step 3: Load to PostgreSQL
# ----------------------------
def load_to_postgres(df, table_name, engine):
    """Load a DataFrame to a PostgreSQL table."""
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Loaded {len(df)} rows into table: {table_name}")

# ----------------------------
# Main pipeline
# ----------------------------
if __name__ == "__main__":
    # Create database connection
    engine = create_engine(
        f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
    )

    # Run ETL pipeline
    cms_raw     = load_raw_data(CMS_FILE)
    cms_clean   = clean_cms_data(cms_raw)
    train_data  = load_raw_data(TRAIN_FILE)
    test_data   = load_raw_data(TEST_FILE)

    load_to_postgres(cms_raw,   "fy",       engine)
    load_to_postgres(cms_clean, "fy_clean", engine)
    load_to_postgres(train_data, "train_df", engine)
    load_to_postgres(test_data,  "test_df",  engine)

    print("ETL pipeline complete.")
```

See `/python/etl_pipeline.py` for the full script.

---

## Cross-Database Compatibility

This project was built on PostgreSQL. The core queries are written to be portable, but enterprise environments — including many federal healthcare systems — use Oracle or MS SQL Server. Below are the key syntax differences to be aware of when migrating queries:

| Operation | PostgreSQL | Oracle | MS SQL Server |
|-----------|-----------|--------|---------------|
| Auto-increment column | `SERIAL` | `GENERATED ALWAYS AS IDENTITY` | `IDENTITY(1,1)` |
| Date conversion | `TO_DATE(col, 'MM/DD/YYYY')` | `TO_DATE(col, 'MM/DD/YYYY')` | `CONVERT(DATE, col, 101)` |
| String concatenation | `\|\|` | `\|\|` | `+` |
| Top N rows | `LIMIT 10` | `FETCH FIRST 10 ROWS ONLY` | `TOP 10` |
| Median | `PERCENTILE_CONT(0.5)` | `PERCENTILE_CONT(0.5)` | `PERCENTILE_CONT(0.5)` (same) |
| Cast | `col::INTEGER` | `CAST(col AS INTEGER)` | `CAST(col AS INTEGER)` |
| Current date | `CURRENT_DATE` | `SYSDATE` | `GETDATE()` |

Window functions, CTEs, and CASE WHEN syntax used in this project are ANSI SQL standard and work identically across PostgreSQL, Oracle, and MS SQL Server.

---

## Integrating Disparate Data Sources

A core responsibility in federal healthcare data engineering is identifying opportunities to integrate disparate data sources — breaking down silos to create a unified, analysis-ready data ecosystem. The current project demonstrates this principle and is designed to be extended with additional data sources.

**Current data sources integrated:**
- CMS HRRP hospital-level data (aggregate facility performance)
- Patient-level clinical data (individual encounter records)

**Planned integrations for a production VA environment:**

| Data Source | Type | Integration Value |
|-------------|------|------------------|
| VA CPRS / CDW (Corporate Data Warehouse) | Clinical data warehouse | Patient encounter, diagnosis, and medication data for Veterans |
| CMS Medicare claims data | Claims / transactional | Actual readmission events linked to Medicare billing records |
| CDC PLACES dataset | Public health / geographic | Social determinants of health at the county level |
| VA facility location data | Reference / dimension | Enrich `dim_facility` with VISN region, urban/rural classification |
| NLP-extracted clinical notes | Unstructured text | Discharge summary risk factors for readmission prediction |

**Integration strategy:** Each new source would follow the same pattern established in this project — raw ingestion into staging tables, cleaning into a `_clean` layer, and loading into the star schema as new dimension or fact tables. Foreign key relationships to existing dimensions (e.g., `dim_facility` via `facility_id`) ensure that new data integrates without breaking existing reports.

---

## How to Run This Project

**Prerequisites:**
- PostgreSQL installed and running
- DBeaver installed and connected to PostgreSQL
- Tableau Desktop or Tableau Public installed
- Python 3.8+ with pandas, psycopg2, sqlalchemy installed

**Steps:**
1. Clone this repository
```bash
git clone https://github.com/yourusername/hospital-readmissions-analytics-pipeline.git
```

2. Option A — Manual import: Import the CSV files into PostgreSQL using DBeaver's import wizard

   Option B — Python ETL: Run the automated pipeline
```bash
pip install pandas psycopg2 sqlalchemy
python python/etl_pipeline.py
```

3. Run the SQL scripts in this order:
```
sql/cleaning/clean_fy_data.sql
sql/schema/create_star_schema.sql
sql/data_quality/all_checks.sql
sql/analysis/descriptive_statistics.sql
sql/analysis/bottleneck_detection.sql
sql/analysis/sla_violations.sql
sql/analysis/state_performance.sql
sql/analysis/performance_tiers.sql
sql/analysis/top_worst_performers.sql
```

4. Open Tableau and connect to your PostgreSQL database using the star schema tables

---

## Future Improvements

- Migrate pipeline to Oracle or MS SQL Server to match enterprise federal healthcare environments
- Implement dbt (data build tool) for version-controlled, tested SQL transformations
- Build supervised ML model using `train_df` with XGBoost to predict patient readmission risk
- Fine-tune a BioBERT or ClinicalBERT model on VA clinical notes for NLP-based readmission risk scoring
- Deploy to AWS GovCloud or Azure Government with FedRAMP-compliant architecture
- Integrate CDC PLACES dataset to add social determinants of health as model features
- Add Apache Spark layer for processing at scale when data volume exceeds single-node PostgreSQL limits
- Build Power BI version of dashboard for environments where Tableau is not available
- Implement Apache Atlas or Microsoft Purview for automated data lineage tracking
- Add year-over-year trend analysis as new CMS fiscal year data is released

---

---

*Data Source: Centers for Medicare & Medicaid Services (CMS) — Hospital Readmissions Reduction Program FY2026*
