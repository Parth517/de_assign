# Git Commit Plan





## Commit Sequence Overview

```text
 1. chore(setup)         -> Repository scaffolding, dependencies, and data-file .gitignore
 2. feat(infra)          -> Snowflake Database, Schemas, Warehouse, and Stage DDL
 3. feat(bronze)         -> Schema-on-read landing tables (RAW_MEMBERS & RAW_REDEMPTIONS)
 4. feat(bronze)         -> Resilient COPY INTO pipelines with dynamic column absorption
 5. feat(silver)         -> Staging table definitions and STG_MEMBERS_REJECTED audit queue
 6. feat(silver)         -> Dynamic REF_COUNTRY_LOOKUP catalog and seed data
 7. feat(transform)      -> Multi-format date inference parser (handling dropped zeros)
 8. feat(transform)      -> Derived business attributes (AGE and 90-day STALE_MEMBER indicator)
 9. feat(transform)      -> Automated data quality quarantine routing with reason codes
10. feat(json)           -> Double LATERAL FLATTEN extraction for nested redemption JSON
11. feat(json)           -> Enriched transaction analytics joining member profile dimensions
12. feat(scd2)           -> Dynamic territory split procedure with SCD-2 relocation tracking
13. feat(gold)           -> Autonomous regional consumption marts with micro-partition clustering
14. feat(marts)          -> Conformed FACT_MEMBER_REDEMPTIONS analytical fact mart
15. feat(quality)        -> Automated data quality audit suite and constraint verification
16. feat(orchestration)  -> Unified end-to-end master runner and Python execution orchestrator
17. docs                 -> Medallion architecture blueprint, dynamic design, and runbook
```

---

## Step-by-Step Commit Instructions

### Commit 1: Project Setup & Repository Scaffolding
* **Objective**: Establish project dependencies, environment configuration, and strict `.gitignore` preventing data file leakage.
* **Stage Command**:
  ```bash
  git add .gitignore requirements.txt
  ```
* **Commit Command**:
  ```bash
  git commit -m "chore(setup): configure repository structure, environment dependencies, and gitignore

  - Add Snowflake connector and verification dependencies in requirements.txt
  - Configure .gitignore to exclude all raw data files (*.csv, *.json, *.xlsx, *.dat), virtualenvs, and credentials"
  ```

---

### Commit 2: Cloud Data Warehouse Infrastructure DDL
* **Objective**: Initialize Snowflake compute warehouse, database, schema isolation, and storage stage.
* **Stage Command**:
  ```bash
  git add sql/01_raw/raw_ddl.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(infra): establish snowflake database, schemas, warehouse, and stage infrastructure

  - Create SKYPOINTS_DB database with isolated RAW, STG, and PROD schemas
  - Configure SKYPOINTS_WH compute warehouse with auto-suspend and auto-resume
  - Establish FF_PIPE_DELIMITED and FF_JSON file formats and internal storage stage"
  ```

---

### Commit 3: Bronze Layer Landing Tables
* **Objective**: Define resilient landing tables utilizing schema-on-read patterns to absorb dirty inbound feeds.
* **Stage Command**:
  ```bash
  git add sql/01_raw/raw_ddl.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(bronze): define schema-on-read landing tables for raw members and json redemptions

  - Create RAW.RAW_MEMBERS with loose VARCHAR typing to prevent data landing failures
  - Create RAW.RAW_REDEMPTIONS with native VARIANT column preserving full JSON payload integrity
  - Include batch tracking tokens and ingestion timestamps across all landing tables"
  ```

---

### Commit 4: Bronze Ingestion Pipelines
* **Objective**: Implement robust `COPY INTO` commands with dynamic column absorption and regex pattern matching.
* **Stage Command**:
  ```bash
  git add sql/01_raw/raw_ddl.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(bronze): implement resilient copy into pipelines with dynamic column absorption

  - Configure COPY INTO for flat files with dynamic column fallback ($10 vs $12 for POST_CODE)
  - Configure COPY INTO for semi-structured JSON redemptions with file pattern regex
  - Enforce ON_ERROR = 'CONTINUE' and capture METADATA$FILENAME and batch identifiers"
  ```

---

### Commit 5: Silver Tier Staging Tables & Dead Letter Queue
* **Objective**: Create clean staging tables with strongly typed schemas and an isolated audit rejection store.
* **Stage Command**:
  ```bash
  git add sql/02_staging/01_stg_ddl.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(silver): create staging table definitions and dead-letter rejection audit queue

  - Define STG.STG_MEMBERS with strongly typed dates, postal codes, and validation status flags
  - Define STG.STG_MEMBERS_REJECTED (Dead Letter Queue) preserving raw unparsed strings
  - Apply micro-partition clustering on BATCH_ID for efficient batch-level purging and inspection"
  ```

---

### Commit 6: Dynamic Geographic Reference Catalog
* **Objective**: Build and seed an alias lookup catalog to eliminate regional naming fragmentation.
* **Stage Command**:
  ```bash
  git add sql/02_staging/01_stg_ddl.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(silver): establish dynamic geographic lookup catalog for country alias resolution

  - Create STG.REF_COUNTRY_LOOKUP table mapping regional aliases to canonical ISO Alpha-3 codes
  - Seed international aliases (India, Bharat, Indi -> IND; Oz, AUST -> AUS; CDA -> CAN; America -> USA)
  - Implement idempotent MERGE ensuring safe re-execution without duplicate key conflicts"
  ```

---

### Commit 7: Intelligent Multi-Format Date Inference Engine
* **Objective**: Implement multi-branch date parser capable of handling disparate formats and dropped leading zeros.
* **Stage Command**:
  ```bash
  git add sql/02_staging/02_stg_transform_load.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(transform): implement intelligent multi-variant date inference engine

  - Support ISO (YYYY-MM-DD), numeric (YYYYMMDD, MMDDYYYY), and European (DDMMYYYY) formats
  - Add length-based leading zero restoration for dropped digit birth dates (3051985 -> 03051985)
  - Disambiguate 7-digit enrollment dates to prevent erroneous 4-digit century mapping (e.g. 1052-02-02)"
  ```

---

### Commit 8: Dynamic Operational & Behavioral Metrics
* **Objective**: Calculate dynamic customer metrics evaluated at runtime against current business date.
* **Stage Command**:
  ```bash
  git add sql/02_staging/02_stg_transform_load.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(transform): calculate dynamic member age and 90-day flight inactivity indicator

  - Derive AGE dynamically via DATEDIFF('year', DOB_PARSED, CURRENT_DATE())
  - Compute STALE_MEMBER indicator evaluating last flight activity against a 90-day recency window
  - Assign 'U' (Unknown) state for members who have never completed a flight"
  ```

---

### Commit 9: Quality Quarantine Engine (Dead Letter Queue)
* **Objective**: Implement comprehensive validation rules routing corrupt records to the rejection table.
* **Stage Command**:
  ```bash
  git add sql/02_staging/02_stg_transform_load.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(transform): implement automated rejection quarantine routing with reason codes

  - Enforce mandatory constraints: non-null and non-zero MEMBER_ID, valid MEMBER_NAME, and COUNTRY
  - Enforce temporal chronology: enrollment date must precede or equal last flight date
  - Guard against future events (enrollment/flight in future) and unrealistic age thresholds (>120 years)
  - Populate STG_MEMBERS_REJECTED with pipe-delimited audit reason codes"
  ```

---

### Commit 10: Nested JSON Flattening & Redemption Relational Load
* **Objective**: Extract 2-level nested JSON arrays into relational transaction rows using double LATERAL FLATTEN.
* **Stage Command**:
  ```bash
  git add sql/02_staging/04_json_redemptions.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(json): flatten nested partner redemption payloads using lateral flatten

  - Implement double LATERAL FLATTEN unpacking outer member array and inner redemptions array
  - Accommodate both single-root JSON objects and array payloads via conditional array constructor
  - Deploy idempotent MERGE on TXN_ID handling late-arriving status transitions (PENDING -> COMPLETED)"
  ```

---

### Commit 11: Member 360 Redemption Analytical Queries
* **Objective**: Implement conformed transaction join queries fulfilling Assessment Deliverable 4.
* **Stage Command**:
  ```bash
  git add sql/02_staging/04_json_redemptions.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(json): create enriched transaction analytics query joining member profile dimensions

  - Join STG_REDEMPTIONS to deduplicated valid member profiles at transaction grain
  - Enrich redemptions with customer tier code, age, activity status, state, and country
  - Form the relational basis for downstream production fact mart synchronization"
  ```

---

### Commit 12: Dynamic Regional Territory Provisioning (SCD Type 2)
* **Objective**: Build stored procedure `SP_CREATE_AND_LOAD_COUNTRY_TABLES()` for autonomous territory splitting.
* **Stage Command**:
  ```bash
  git add sql/02_staging/03_dynamic_country_split.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(scd2): develop dynamic regional table provisioning with customer migration tracking

  - Deploy SP_CREATE_AND_LOAD_COUNTRY_TABLES to auto-provision STG_MEMBERS_<COUNTRY> at runtime
  - Eliminate hardcoded country names: dynamically discover territories from inbound feeds
  - Implement SCD Type 2 relocation logic: expire prior territory (IS_ACTIVE=0) and activate new (IS_ACTIVE=1)
  - Order window functions by business effective event date rather than ingestion clock"
  ```

---

### Commit 13: Gold Tier Regional Consumption Marts
* **Objective**: Provision production consumer tables `PROD.TABLE_<COUNTRY>` with micro-partition clustering.
* **Stage Command**:
  ```bash
  git add sql/03_targets/target_tables.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(gold): create dynamically provisioned regional consumption marts

  - Deploy PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES to create and synchronize PROD.TABLE_<COUNTRY>
  - Configure clustering keys on (MEMBER_ID) for high-performance point lookups
  - Implement atomic TRUNCATE and bulk reload from Silver country master"
  ```

---

### Commit 14: Enriched Redemption Fact Mart (Assessment Deliverable 4)
* **Objective**: Build production fact mart `PROD.FACT_MEMBER_REDEMPTIONS` joined with member dimensions.
* **Stage Command**:
  ```bash
  git add sql/03_targets/target_tables.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(marts): construct unified partner redemption fact table with clustering keys

  - Create PROD.FACT_MEMBER_REDEMPTIONS joining partner airline events with member dimensions
  - Cluster on (TXN_DATE, MEMBER_ID) to maximize micro-partition pruning on analytical queries
  - Apply idempotent MERGE logic updating transaction fulfillment state and point values"
  ```

---

### Commit 15: Automated Quality Gate & Data Integrity Suite
* **Objective**: Configure automated validation suite verifying constraints, chronology, and orphan records.
* **Stage Command**:
  ```bash
  git add sql/05_validations/validations.sql
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(quality): configure automated data quality audit gate and constraint verification

  - Assert mandatory constraints: zero missing IDs, zero unmapped country records
  - Verify temporal integrity: enrollment precedes last flight date and zero future dates
  - Validate partner redemption integrity: positive mileage values and status code conformity
  - Structure output with standardized SEVERITY, RECORD_COUNT, and SAMPLE_VALUES columns"
  ```

---

### Commit 16: Unified Master Orchestration Pipeline & Python Runner
* **Objective**: Assemble sequential end-to-end runner and Snowflake Task DAG blueprint for automated execution.
* **Stage Command**:
  ```bash
  git add master_pipeline.sql run_pipeline.py
  ```
* **Commit Command**:
  ```bash
  git commit -m "feat(orchestration): assemble unified end-to-end master pipeline and python runner

  - Create master_pipeline.sql running Bronze Ingest -> Silver Flatten & Cleansing -> Gold Marts
  - Incorporate non-mandatory POST_CODE column and schema evolution across all pipeline steps
  - Define Snowflake Task DAG blueprint (CRON 0 2 * * * UTC) for automated daily scheduling
  - Add run_pipeline.py for automated command-line execution and CI/CD integration"
  ```

---

### Commit 17: Enterprise Architecture Blueprint & Runbook
* **Objective**: Deliver comprehensive documentation detailing Medallion flow, dynamic design, and AI usage.
* **Stage Command**:
  ```bash
  git add architecture_diagram.md README.md commit_plan.md
  ```
* **Commit Command**:
  ```bash
  git commit -m "docs: publish architectural blueprints, dynamic design guide, and operating runbook

  - Publish architecture_diagram.md with ASCII Medallion flows, SCD-2 state diagrams, and topology
  - Update README.md documenting 100% dynamic architecture (zero-hardcoding principle)
  - Detail AI-assisted engineering methodology leveraging Gemini 3.8 and Claude Sonnet 4.7
  - Add commit_plan.md outlining standard Conventional Commits strategy"
  ```


