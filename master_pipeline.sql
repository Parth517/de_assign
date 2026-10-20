-- =============================================================================
-- FILE: 06_pipeline/master_pipeline.sql
-- LAYER: ORCHESTRATION / END-TO-END PIPELINE RUNNER
-- PURPOSE:
--   Executes the entire SkyPoints Medallion pipeline in strict succession:
--
--     Step 1: RAW INGESTION      (01_raw/raw_ddl.sql)
--             -> Ingests flat file + JSON into RAW_MEMBERS and RAW_REDEMPTIONS
--
--     Step 2: JSON FLATTENING    (04_json/json_redemptions.sql)
--             -> Flattens nested JSON redemption transactions into STG_REDEMPTIONS
--
--     Step 3: SILVER TRANSFORMS  (02_staging/02_stg_transform_load.sql)
--             -> Date parsing, AGE, STALE_MEMBER, NULL Country handling, Audit
--
--     Step 4: DYNAMIC SPLIT      (02_staging/03_dynamic_country_split.sql)
--             -> SP dynamically provisions STG_MEMBERS_<COUNTRY> with SCD Type 2
--                migration tracking: IS_ACTIVE=0 and END_DATE in previous country;
--                IS_ACTIVE=1 and END_DATE=NULL in current country
--
--     Step 5: QUALITY GATE       (05_validations/validations.sql)
--             -> Validates mandatory fields, uniqueness, format integrity
--
--     Step 6: GOLD TARGETS       (03_targets/target_tables.sql)
--             -> SP dynamically provisions and copies clean data into PROD.TABLE_<COUNTRY>
--                preserving IS_ACTIVE, START_DATE, and END_DATE columns
--
-- INCLUDES:
--   Part A: Interactive / Sequential SQL Runner (for worksheets & CI/CD)
--   Part B: Automated Snowflake Task DAG (for production hands-free scheduling)
-- =============================================================================

USE DATABASE SKYPOINTS_DB;
USE WAREHOUSE SKYPOINTS_WH;


-- =============================================================================
-- PART A: SEQUENTIAL PIPELINE EXECUTION
-- =============================================================================

-- ── 0. GENERATE UNIFIED PIPELINE BATCH ID ─────────────────────────────────────
-- Timestamp-based token (YYYYMMDD_HH24MISS) stamped across all layers
SET BATCH_ID = TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS');
SET ACTIVE_BATCH_ID = $BATCH_ID;
SET BATCH_IN_RAW    = $BATCH_ID;
SELECT $BATCH_ID AS PIPELINE_RUN_BATCH_ID;


-- ── 1. RAW INGESTION (BRONZE) ────────────────────────────────────────────────
USE SCHEMA RAW;

-- Ensure schema evolution for POST_CODE column if RAW table was created earlier
ALTER TABLE IF EXISTS RAW.RAW_MEMBERS ADD COLUMN IF NOT EXISTS POST_CODE VARCHAR(10);

-- 1a. Ingest pipe-delimited flat file (members)
COPY INTO RAW.RAW_MEMBERS (
    REC_TYPE, MEMBER_NAME, MEMBER_ID, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    TIER_CODE, AGENT_NAME, STATE, COUNTRY, POST_CODE, DOB, IS_ACTIVE,
    SOURCE_FILE, BATCH_ID
)
FROM (
    SELECT
        $1, $2, $3, $4, $5, $6, $7, $8,
        $9,                                              -- COUNTRY
        CASE WHEN $12 IS NOT NULL THEN $10 ELSE $12 END, -- POST_CODE (if 12 cols, $10 is Post Code; if 11 cols, NULL)
        CASE WHEN $12 IS NOT NULL THEN $11 ELSE $10 END, -- DOB
        CASE WHEN $12 IS NOT NULL THEN $12 ELSE $11 END, -- IS_ACTIVE
        METADATA$FILENAME,
        $BATCH_ID
    FROM @RAW.SKYPOINTS_STAGE
    (PATTERN => '.*members.*\\.(csv|dat|txt)')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_PIPE_DELIMITED')
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;

-- 1b. Ingest JSON redemption feed
COPY INTO RAW.RAW_REDEMPTIONS (JSON_PAYLOAD, SOURCE_FILE, BATCH_ID)
FROM (
    SELECT $1, METADATA$FILENAME, $BATCH_ID
    FROM @RAW.SKYPOINTS_STAGE
    (PATTERN => '.*redemptions.*\.json')
)
FILE_FORMAT = (FORMAT_NAME = 'RAW.FF_JSON')
ON_ERROR    = 'CONTINUE'
FORCE       = TRUE;


-- ── 2. JSON PROCESSING (SILVER) ──────────────────────────────────────────────
USE SCHEMA STG;

CREATE OR REPLACE TABLE STG.STG_REDEMPTIONS (
    MEMBER_ID           VARCHAR(20)     NOT NULL,
    FEED_DATE           DATE,
    TXN_ID              VARCHAR(50)     NOT NULL,
    TXN_DATE            DATE,
    PARTNER             VARCHAR(100),
    MILES_REDEEMED      NUMBER(12),
    STATUS              VARCHAR(20),
    SOURCE_FILE         VARCHAR(500),
    LOAD_TS             TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    BATCH_ID            VARCHAR(50)
)
CLUSTER BY (BATCH_ID, MEMBER_ID)
COMMENT = 'Silver: physical table of flattened partner airline redemption transactions.';

-- 2b. Idempotent MERGE into STG_REDEMPTIONS (seamlessly handles late-arriving redemptions & updates)
MERGE INTO STG.STG_REDEMPTIONS tgt
USING (
    WITH latest_batch AS (
        SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') AS BATCH_ID FROM RAW.RAW_REDEMPTIONS
    )
    SELECT
        m.VALUE:member_id::VARCHAR(20)                               AS MEMBER_ID,
        TRY_TO_DATE(m.VALUE:feed_date::VARCHAR, 'YYYYMMDD')          AS FEED_DATE,
        f.VALUE:txn_id::VARCHAR(50)                                  AS TXN_ID,
        TRY_TO_DATE(f.VALUE:txn_date::VARCHAR, 'YYYYMMDD')           AS TXN_DATE,
        f.VALUE:partner::VARCHAR(100)                                AS PARTNER,
        f.VALUE:miles_redeemed::NUMBER(12)                           AS MILES_REDEEMED,
        f.VALUE:status::VARCHAR(20)                                  AS STATUS,
        r.SOURCE_FILE,
        COALESCE(r.BATCH_ID, b.BATCH_ID)                             AS BATCH_ID
    FROM RAW.RAW_REDEMPTIONS r
    CROSS JOIN latest_batch b,
         LATERAL FLATTEN(
             INPUT => CASE 
                 WHEN IS_ARRAY(r.JSON_PAYLOAD) THEN r.JSON_PAYLOAD 
                 ELSE ARRAY_CONSTRUCT(r.JSON_PAYLOAD) 
             END
         ) m,
         LATERAL FLATTEN(INPUT => m.VALUE:redemptions) f
    WHERE (r.BATCH_ID = b.BATCH_ID OR r.BATCH_ID IS NULL OR b.BATCH_ID = 'DEFAULT_BATCH')
) src
ON tgt.TXN_ID = src.TXN_ID
WHEN MATCHED AND (tgt.STATUS <> src.STATUS OR tgt.MILES_REDEEMED <> src.MILES_REDEEMED) THEN
    UPDATE SET
        tgt.STATUS         = src.STATUS,
        tgt.MILES_REDEEMED = src.MILES_REDEEMED,
        tgt.FEED_DATE      = src.FEED_DATE,
        tgt.BATCH_ID       = src.BATCH_ID,
        tgt.LOAD_TS        = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (
        MEMBER_ID, FEED_DATE, TXN_ID, TXN_DATE, PARTNER, MILES_REDEEMED, STATUS,
        SOURCE_FILE, BATCH_ID
    )
    VALUES (
        src.MEMBER_ID, src.FEED_DATE, src.TXN_ID, src.TXN_DATE, src.PARTNER, src.MILES_REDEEMED, src.STATUS,
        src.SOURCE_FILE, src.BATCH_ID
    );


-- ── 3. SILVER DATA TRANSFORMATION & AUDIT ─────────────────────────────────────
USE SCHEMA STG;

-- Ensure schema evolution for POST_CODE column if Silver tables were created earlier
ALTER TABLE IF EXISTS STG.STG_MEMBERS ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5);
ALTER TABLE IF EXISTS STG.STG_MEMBERS_REJECTED ADD COLUMN IF NOT EXISTS POST_CODE_RAW VARCHAR(10);

-- 3a. Idempotent cleanup for this batch
DELETE FROM STG.STG_MEMBERS WHERE BATCH_ID = $ACTIVE_BATCH_ID;

-- 3b. Transform, derive AGE & STALE_MEMBER, validate and push to STG_MEMBERS
INSERT INTO STG.STG_MEMBERS (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
    IS_VALID, REJECTION_REASON, SOURCE_FILE, LOAD_TS, BATCH_ID
)
WITH raw_normalized AS (
    SELECT
        TRIM(MEMBER_ID)                                 AS MEMBER_ID,
        TRIM(MEMBER_NAME)                               AS MEMBER_NAME,
        UPPER(TRIM(TIER_CODE))                          AS TIER_CODE,
        TRIM(AGENT_NAME)                                AS AGENT_NAME,
        TRIM(STATE)                                     AS STATE,
        -- Handle optional Country: fallback to filename or keep NULL
        COALESCE(
            NULLIF(TRIM(COUNTRY), ''),
            CASE
                WHEN UPPER(SOURCE_FILE) LIKE '%IND%'   THEN 'IND'
                WHEN UPPER(SOURCE_FILE) LIKE '%USA%'   THEN 'USA'
                WHEN UPPER(SOURCE_FILE) LIKE '%AUS%'   THEN 'AUS'
                WHEN UPPER(SOURCE_FILE) LIKE '%CAN%'   THEN 'CAN'
                WHEN UPPER(SOURCE_FILE) LIKE '%PHIL%'  THEN 'PHIL'
                ELSE NULL
            END
        )                                               AS COUNTRY,
        TRY_TO_NUMBER(TRIM(POST_CODE))                  AS POST_CODE,
        TRIM(POST_CODE)                                 AS POST_CODE_RAW,
        NULLIF(TRIM(IS_ACTIVE), '')                     AS IS_ACTIVE,
        NULL::VARCHAR(1)                                AS MEMBER_TYPE,
        -- DOB: spec format is DDMMYYYY (e.g. 03051985 = 3 May 1985; also handles 7-digit 3051985)
        COALESCE(
            TRY_TO_DATE(TRIM(DOB), 'DDMMYYYY'),
            CASE WHEN LENGTH(TRIM(DOB)) = 7 THEN TRY_TO_DATE('0' || TRIM(DOB), 'DDMMYYYY') END
        )                                               AS DOB_PARSED,
        TRIM(DOB)                                       AS DOB_RAW,

        -- Enrollment date: Intelligent multi-format parser avoiding false year matches (e.g. 1052022 -> 2022-10-05, NOT 1052-02-02)
        CASE
            WHEN TRIM(ENROLLMENT_DATE) LIKE '____-__-__'
                 THEN TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYY-MM-DD')

            WHEN LENGTH(TRIM(ENROLLMENT_DATE)) = 8 AND REGEXP_LIKE(TRIM(ENROLLMENT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN SUBSTR(TRIM(ENROLLMENT_DATE), 1, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'YYYYMMDD') END,
                     CASE WHEN SUBSTR(TRIM(ENROLLMENT_DATE), 5, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(ENROLLMENT_DATE), 'MMDDYYYY') END
                 )

            WHEN LENGTH(TRIM(ENROLLMENT_DATE)) = 7 AND REGEXP_LIKE(TRIM(ENROLLMENT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN TRY_TO_NUMBER(SUBSTR(TRIM(ENROLLMENT_DATE), 1, 2)) BETWEEN 1 AND 12
                           AND SUBSTR(TRIM(ENROLLMENT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(ENROLLMENT_DATE), 4, 4) || '-' ||
                              SUBSTR(TRIM(ENROLLMENT_DATE), 1, 2) || '-0' ||
                              SUBSTR(TRIM(ENROLLMENT_DATE), 3, 1),
                              'YYYY-MM-DD'
                          ) END,
                     CASE WHEN SUBSTR(TRIM(ENROLLMENT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(ENROLLMENT_DATE), 4, 4) || '-0' ||
                              SUBSTR(TRIM(ENROLLMENT_DATE), 1, 1) || '-' ||
                              SUBSTR(TRIM(ENROLLMENT_DATE), 2, 2),
                              'YYYY-MM-DD'
                          ) END
                 )
        END                                             AS ENROLLMENT_DATE_PARSED,
        TRIM(ENROLLMENT_DATE)                           AS ENROLLMENT_DATE_RAW,

        -- Last flight date: same intelligent multi-format parser
        CASE
            WHEN TRIM(LAST_FLIGHT_DATE) LIKE '____-__-__'
                 THEN TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYY-MM-DD')

            WHEN LENGTH(TRIM(LAST_FLIGHT_DATE)) = 8 AND REGEXP_LIKE(TRIM(LAST_FLIGHT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN SUBSTR(TRIM(LAST_FLIGHT_DATE), 1, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'YYYYMMDD') END,
                     CASE WHEN SUBSTR(TRIM(LAST_FLIGHT_DATE), 5, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(LAST_FLIGHT_DATE), 'MMDDYYYY') END
                 )

            WHEN LENGTH(TRIM(LAST_FLIGHT_DATE)) = 7 AND REGEXP_LIKE(TRIM(LAST_FLIGHT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN TRY_TO_NUMBER(SUBSTR(TRIM(LAST_FLIGHT_DATE), 1, 2)) BETWEEN 1 AND 12
                           AND SUBSTR(TRIM(LAST_FLIGHT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(LAST_FLIGHT_DATE), 4, 4) || '-' ||
                              SUBSTR(TRIM(LAST_FLIGHT_DATE), 1, 2) || '-0' ||
                              SUBSTR(TRIM(LAST_FLIGHT_DATE), 3, 1),
                              'YYYY-MM-DD'
                          ) END,
                     CASE WHEN SUBSTR(TRIM(LAST_FLIGHT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(LAST_FLIGHT_DATE), 4, 4) || '-0' ||
                              SUBSTR(TRIM(LAST_FLIGHT_DATE), 1, 1) || '-' ||
                              SUBSTR(TRIM(LAST_FLIGHT_DATE), 2, 2),
                              'YYYY-MM-DD'
                          ) END
                 )
        END                                             AS LAST_FLIGHT_DATE_PARSED,
        TRIM(LAST_FLIGHT_DATE)                          AS LAST_FLIGHT_DATE_RAW,
        COALESCE(SOURCE_FILE, 'MEMBERS_FEED')           AS SOURCE_FILE,
        COALESCE(LOAD_TS, CURRENT_TIMESTAMP())          AS LOAD_TS,
        $ACTIVE_BATCH_ID                                AS BATCH_ID
    FROM RAW.RAW_MEMBERS
    WHERE UPPER(TRIM(COALESCE(REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
      AND (
          ($BATCH_IN_RAW IS NOT NULL AND BATCH_ID = $BATCH_IN_RAW)
          OR ($BATCH_IN_RAW IS NULL)
      )
),
with_derived AS (
    SELECT
        *,
        CASE WHEN DOB_PARSED IS NOT NULL THEN DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) END AS AGE,
        CASE
            WHEN LAST_FLIGHT_DATE_PARSED IS NULL THEN 'U'
            WHEN DATEDIFF('day', LAST_FLIGHT_DATE_PARSED, CURRENT_DATE()) > 90 THEN 'Y'
            ELSE 'N'
        END AS STALE_MEMBER,
        ARRAY_TO_STRING(
            ARRAY_CONSTRUCT_COMPACT(
                CASE WHEN MEMBER_ID IS NULL OR TRIM(MEMBER_ID) IN ('', '0', '00000000') THEN 'MISSING_OR_ZERO_MEMBER_ID' END,
                CASE WHEN MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = '' THEN 'MISSING_MEMBER_NAME' END,
                CASE WHEN COUNTRY IS NULL OR TRIM(COUNTRY) IN ('', 'UNKNOWN') THEN 'MISSING_COUNTRY' END,
                CASE WHEN TIER_CODE NOT IN ('PLT','GLD','SLV','BRZ') THEN 'INVALID_TIER_CODE' END,
                CASE WHEN IS_ACTIVE IS NULL THEN 'MISSING_IS_ACTIVE' END,
                CASE WHEN ENROLLMENT_DATE_RAW IS NULL OR TRIM(ENROLLMENT_DATE_RAW) IN ('', '0', '00000000') THEN 'MISSING_OR_ZERO_ENROLLMENT_DATE' END,
                CASE WHEN ENROLLMENT_DATE_PARSED IS NULL THEN 'INVALID_ENROLLMENT_DATE' END,
                CASE WHEN DOB_RAW IS NOT NULL AND TRIM(DOB_RAW) NOT IN ('', '0', 'NULL') AND DOB_PARSED IS NULL THEN 'INVALID_DOB_FORMAT' END,
                CASE WHEN LAST_FLIGHT_DATE_RAW IS NOT NULL AND TRIM(LAST_FLIGHT_DATE_RAW) NOT IN ('', '0', 'NULL') AND LAST_FLIGHT_DATE_PARSED IS NULL THEN 'INVALID_FLIGHT_DATE' END,
                CASE WHEN ENROLLMENT_DATE_PARSED IS NOT NULL
                      AND LAST_FLIGHT_DATE_PARSED IS NOT NULL
                      AND ENROLLMENT_DATE_PARSED > LAST_FLIGHT_DATE_PARSED THEN 'ENROLLMENT_AFTER_FLIGHT' END,
                CASE WHEN DOB_PARSED IS NOT NULL
                      AND (DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) < 0
                        OR DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) > 120) THEN 'UNREALISTIC_AGE' END,
                CASE WHEN ENROLLMENT_DATE_PARSED > CURRENT_DATE() THEN 'ENROLLMENT_DATE_IN_FUTURE' END,
                CASE WHEN LAST_FLIGHT_DATE_PARSED > CURRENT_DATE() THEN 'FLIGHT_DATE_IN_FUTURE' END
            ),
            '|'
        ) AS REJECTION_REASON
    FROM raw_normalized
)
SELECT
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB_PARSED, ENROLLMENT_DATE_PARSED, LAST_FLIGHT_DATE_PARSED,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
    CASE WHEN REJECTION_REASON IS NULL OR REJECTION_REASON = '' THEN TRUE ELSE FALSE END AS IS_VALID,
    NULLIF(REJECTION_REASON, '') AS REJECTION_REASON,
    SOURCE_FILE, LOAD_TS, BATCH_ID
FROM with_derived;

-- 3c. Idempotent load into STG_MEMBERS_REJECTED (Guarantees zero duplicates on re-runs)
DELETE FROM STG.STG_MEMBERS_REJECTED 
WHERE BATCH_ID = $ACTIVE_BATCH_ID OR ($BATCH_IN_RAW IS NULL);

INSERT INTO STG.STG_MEMBERS_REJECTED (
    MEMBER_ID, MEMBER_NAME, TIER_CODE,
    DOB_RAW, ENROLLMENT_DATE_RAW, LAST_FLIGHT_DATE_RAW,
    COUNTRY, POST_CODE_RAW, IS_ACTIVE, REJECTION_REASON,
    SOURCE_FILE, BATCH_ID
)
SELECT
    s.MEMBER_ID,
    s.MEMBER_NAME,
    s.TIER_CODE,
    COALESCE(r.DOB, TO_VARCHAR(s.DOB), '') AS DOB_RAW,
    COALESCE(r.ENROLLMENT_DATE, TO_VARCHAR(s.ENROLLMENT_DATE), '') AS ENROLLMENT_DATE_RAW,
    COALESCE(r.LAST_FLIGHT_DATE, TO_VARCHAR(s.LAST_FLIGHT_DATE), '') AS LAST_FLIGHT_DATE_RAW,
    s.COUNTRY,
    COALESCE(r.POST_CODE, TO_VARCHAR(s.POST_CODE), '') AS POST_CODE_RAW,
    s.IS_ACTIVE,
    s.REJECTION_REASON,
    s.SOURCE_FILE,
    s.BATCH_ID
FROM STG.STG_MEMBERS s
LEFT JOIN (
    SELECT 
        TRIM(MEMBER_ID) AS MEMBER_ID,
        TRIM(DOB) AS DOB,
        TRIM(ENROLLMENT_DATE) AS ENROLLMENT_DATE,
        TRIM(LAST_FLIGHT_DATE) AS LAST_FLIGHT_DATE,
        TRIM(POST_CODE) AS POST_CODE
    FROM RAW.RAW_MEMBERS
    WHERE UPPER(TRIM(COALESCE(REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
    QUALIFY ROW_NUMBER() OVER (PARTITION BY TRIM(MEMBER_ID) ORDER BY COALESCE(LOAD_TS, CURRENT_TIMESTAMP()) DESC) = 1
) r ON s.MEMBER_ID = r.MEMBER_ID
WHERE s.IS_VALID = FALSE;



-- ── 4. DYNAMIC COUNTRY TABLE CREATION & SPLIT (SILVER) ────────────────────────
-- Automatically creates STG_MEMBERS_<COUNTRY> for every country found in data.
-- Implements SCD Type 2 country migration tracking:
--   - In previous country (e.g. IND): IS_ACTIVE = 0 and END_DATE = date of move
--   - In current country (e.g. USA):  IS_ACTIVE = 1 and END_DATE = NULL
CREATE OR REPLACE PROCEDURE STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    c_cursor CURSOR FOR
        SELECT DISTINCT UPPER(TRIM(COUNTRY)) AS COUNTRY
        FROM STG.STG_MEMBERS
        WHERE IS_VALID = TRUE
          AND COUNTRY IS NOT NULL
          AND TRIM(COUNTRY) NOT IN ('', 'UNKNOWN');
    v_country VARCHAR;
    v_sql VARCHAR;
    v_count INT := 0;
BEGIN
    FOR rec IN c_cursor DO
        v_country := rec.COUNTRY;

        -- Step 4a: Create country-specific Silver table dynamically if not exists
        v_sql := 'CREATE TABLE IF NOT EXISTS STG.STG_MEMBERS_' || v_country || ' (
            MEMBER_ID           VARCHAR(20)     NOT NULL,
            MEMBER_NAME         VARCHAR(100),
            TIER_CODE           VARCHAR(10),
            DOB                 DATE,
            ENROLLMENT_DATE     DATE,
            LAST_FLIGHT_DATE    DATE,
            AGE                 NUMBER(3)       COMMENT ''Recomputed fresh from DOB vs CURRENT_DATE each run'',
            STALE_MEMBER        VARCHAR(1)      COMMENT ''Recomputed fresh from LAST_FLIGHT_DATE each run'',
            AGENT_NAME          VARCHAR(100),
            STATE               VARCHAR(50),
            COUNTRY             VARCHAR(10),
            POST_CODE           NUMBER(5)       COMMENT ''Post code (5-digit integer per spec, optional)'',
            IS_ACTIVE           NUMBER(1)       COMMENT ''1 if currently active in this country, 0 if moved to another country'',
            MEMBER_TYPE         VARCHAR(1),
            START_DATE          DATE            COMMENT ''Date member became active in this country'',
            END_DATE            DATE            COMMENT ''Date member moved away; NULL if currently active'',
            SOURCE_FILE         VARCHAR(500),
            BATCH_ID            VARCHAR(50),
            STG_LOAD_TS         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
        ) CLUSTER BY (MEMBER_ID)
        COMMENT = ''Silver: members for country ' || v_country || ' with migration tracking (1=active, 0=moved)''';
        EXECUTE IMMEDIATE v_sql;

        -- Ensure POST_CODE column exists if country table was created previously without it
        v_sql := 'ALTER TABLE STG.STG_MEMBERS_' || v_country || ' ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5)';
        EXECUTE IMMEDIATE v_sql;

        -- Step 4b: Clear country table for fresh idempotent rebuild from full history
        v_sql := 'TRUNCATE TABLE STG.STG_MEMBERS_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        -- Step 4c: Insert members with SCD Type 2 migration tracking
        v_sql := 'INSERT INTO STG.STG_MEMBERS_' || v_country || ' (
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
            START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
        )
        WITH member_history AS (
            SELECT
                MEMBER_ID,
                MEMBER_NAME,
                TIER_CODE,
                DOB,
                ENROLLMENT_DATE,
                LAST_FLIGHT_DATE,
                CASE WHEN DOB IS NOT NULL THEN DATEDIFF(''year'', DOB, CURRENT_DATE()) END AS AGE,
                CASE
                    WHEN LAST_FLIGHT_DATE IS NULL                                THEN ''U''
                    WHEN DATEDIFF(''day'', LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN ''Y''
                    ELSE                                                           ''N''
                END AS STALE_MEMBER,
                AGENT_NAME,
                STATE,
                COUNTRY,
                POST_CODE,
                MEMBER_TYPE,
                SOURCE_FILE,
                BATCH_ID,
                LOAD_TS,
                -- Business Effective Date handles late-arriving out-of-order member feeds:
                COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) AS EFFECTIVE_DATE,
                -- Date member moved to the next country:
                LEAD(COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE)) OVER (
                    PARTITION BY MEMBER_ID 
                    ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) ASC, LOAD_TS ASC
                ) AS NEXT_MOVE_DATE,
                -- RN_GLOBAL = 1: member''''s latest active profile globally by business event time
                ROW_NUMBER() OVER (
                    PARTITION BY MEMBER_ID 
                    ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
                ) AS RN_GLOBAL,
                -- RN_COUNTRY = 1: latest record for this specific country
                ROW_NUMBER() OVER (
                    PARTITION BY MEMBER_ID, COUNTRY 
                    ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
                ) AS RN_COUNTRY
            FROM STG.STG_MEMBERS
            WHERE IS_VALID = TRUE
              AND ENROLLMENT_DATE IS NOT NULL
              AND ENROLLMENT_DATE <= CURRENT_DATE()
              AND (LAST_FLIGHT_DATE IS NULL OR ENROLLMENT_DATE <= LAST_FLIGHT_DATE)
              AND (DOB IS NULL OR (DOB <= CURRENT_DATE() AND DATEDIFF(''year'', DOB, CURRENT_DATE()) BETWEEN 0 AND 120))
              AND MEMBER_ID NOT IN ('''', ''0'', ''00000000'')
        )
        SELECT
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE,
            -- 1 if active in this country, 0 if moved to another country
            CASE WHEN RN_GLOBAL = 1 THEN 1 ELSE 0 END AS IS_ACTIVE,
            MEMBER_TYPE,
            COALESCE(ENROLLMENT_DATE, LOAD_TS::DATE) AS START_DATE,
            CASE WHEN RN_GLOBAL = 1 THEN NULL ELSE COALESCE(NEXT_MOVE_DATE, CURRENT_DATE()) END AS END_DATE,
            SOURCE_FILE,
            BATCH_ID
        FROM member_history
        WHERE RN_COUNTRY = 1
          AND COUNTRY = ''' || v_country || '''';
        EXECUTE IMMEDIATE v_sql;

        v_count := v_count + 1;
    END FOR;

    RETURN 'Success: Dynamically provisioned and loaded ' || v_count || ' per-country tables in STG.';
END;
$$;

CALL STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES();


-- ── 5. DATA QUALITY VALIDATIONS (QUALITY GATE) ────────────────────────────────
-- Quick automated gate: ensure no critical errors before promoting to Gold
SELECT
    'CRITICAL_DEFECT_CHECK' AS CHECK_TYPE,
    COUNT(*) AS DEFECT_COUNT
FROM STG.STG_MEMBERS
WHERE BATCH_ID = $BATCH_ID
  AND IS_VALID = FALSE
  AND (MEMBER_ID IS NULL OR COUNTRY IS NULL);


-- ── 6. GOLD TARGET SYNCHRONIZATION ───────────────────────────────────────────
USE SCHEMA PROD;

-- Dynamically provisions and copies Silver country tables into Gold:
CREATE OR REPLACE PROCEDURE PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    c_cursor CURSOR FOR
        SELECT DISTINCT COUNTRY
        FROM STG.STG_MEMBERS
        WHERE IS_VALID = TRUE
          AND COUNTRY IS NOT NULL
          AND TRIM(COUNTRY) NOT IN ('', 'UNKNOWN');
    v_country VARCHAR;
    v_sql VARCHAR;
    v_count INT := 0;
BEGIN
    FOR rec IN c_cursor DO
        v_country := UPPER(TRIM(rec.COUNTRY));

        -- Step 6a: Create target table dynamically if it does not exist
        v_sql := 'CREATE TABLE IF NOT EXISTS PROD.TABLE_' || v_country || ' (
            MEMBER_ID           VARCHAR(20)     NOT NULL,
            MEMBER_NAME         VARCHAR(100),
            TIER_CODE           VARCHAR(10),
            DOB                 DATE,
            ENROLLMENT_DATE     DATE,
            LAST_FLIGHT_DATE    DATE,
            AGE                 NUMBER(3),
            STALE_MEMBER        VARCHAR(1),
            AGENT_NAME          VARCHAR(100),
            STATE               VARCHAR(50),
            COUNTRY             VARCHAR(10),
            POST_CODE           NUMBER(5)       COMMENT ''Post code (5-digit integer per spec, optional)'',
            IS_ACTIVE           NUMBER(1)       COMMENT ''1 if currently active in this country, 0 if moved to another country'',
            MEMBER_TYPE         VARCHAR(1),
            START_DATE          DATE            COMMENT ''Date member became active in this country'',
            END_DATE            DATE            COMMENT ''Date member moved away; NULL if currently active'',
            SOURCE_FILE         VARCHAR(500),
            BATCH_ID            VARCHAR(50),
            LOADED_AT           TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
        ) CLUSTER BY (MEMBER_ID)
        COMMENT = ''Gold: per-country table for ' || v_country || ' with migration tracking (1=active, 0=moved)''';
        EXECUTE IMMEDIATE v_sql;

        -- Ensure POST_CODE column exists if country table was created previously without it
        v_sql := 'ALTER TABLE PROD.TABLE_' || v_country || ' ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5)';
        EXECUTE IMMEDIATE v_sql;

        -- Step 6b: Clear existing records in this country table for fresh refresh
        v_sql := 'TRUNCATE TABLE PROD.TABLE_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        -- Step 6c: Direct copy from Silver country table (ZERO processing in Gold)
        v_sql := 'INSERT INTO PROD.TABLE_' || v_country || ' (
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
            START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
        )
        SELECT
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
            START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
        FROM STG.STG_MEMBERS_' || v_country || '
        WHERE ENROLLMENT_DATE IS NOT NULL
          AND ENROLLMENT_DATE <= CURRENT_DATE()
          AND (LAST_FLIGHT_DATE IS NULL OR ENROLLMENT_DATE <= LAST_FLIGHT_DATE)
          AND (DOB IS NULL OR (DOB <= CURRENT_DATE() AND DATEDIFF(''year'', DOB, CURRENT_DATE()) BETWEEN 0 AND 120))
          AND MEMBER_ID NOT IN ('''', ''0'', ''00000000'')';
        EXECUTE IMMEDIATE v_sql;

        v_count := v_count + 1;
    END FOR;

    RETURN 'Success: Dynamically provisioned and loaded ' || v_count || ' per-country tables in PROD.';
END;
$$;

CALL PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES();


-- ── 7. SUMMARY REPORT ────────────────────────────────────────────────────────
SELECT 'PIPELINE_COMPLETE' AS STATUS, $BATCH_ID AS PROCESSED_BATCH_ID;





-- =============================================================================
-- PART B: AUTOMATED SNOWFLAKE TASK DAG (Hands-Free Production Schedule)
-- =============================================================================
-- In enterprise production, this DAG runs daily at 02:00 UTC automatically.
-- Each task triggers the next upon successful completion.
-- =============================================================================

/*
-- Root Task: Starts at 02:00 AM UTC
CREATE OR REPLACE TASK SKYPOINTS_TASK_01_RAW_INGEST
    WAREHOUSE = SKYPOINTS_WH
    SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS
    COPY INTO RAW.RAW_MEMBERS ...;

-- Child Task 2: Flattens JSON after Raw Ingest finishes
CREATE OR REPLACE TASK SKYPOINTS_TASK_02_JSON_FLATTEN
    WAREHOUSE = SKYPOINTS_WH
    AFTER SKYPOINTS_TASK_01_RAW_INGEST
AS
    INSERT INTO STG.STG_REDEMPTIONS ...;

-- Child Task 3: Cleanses & derives columns
CREATE OR REPLACE TASK SKYPOINTS_TASK_03_SILVER_TRANSFORM
    WAREHOUSE = SKYPOINTS_WH
    AFTER SKYPOINTS_TASK_02_JSON_FLATTEN
AS
    INSERT INTO STG.STG_MEMBERS ...;

-- Child Task 4: Dynamically splits into country tables
CREATE OR REPLACE TASK SKYPOINTS_TASK_04_COUNTRY_SPLIT
    WAREHOUSE = SKYPOINTS_WH
    AFTER SKYPOINTS_TASK_03_SILVER_TRANSFORM
AS
    CALL STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES();

-- Child Task 5: Loads Gold target tables
CREATE OR REPLACE TASK SKYPOINTS_TASK_05_GOLD_LOAD
    WAREHOUSE = SKYPOINTS_WH
    AFTER SKYPOINTS_TASK_04_COUNTRY_SPLIT
AS
    CALL PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES();

-- To activate the DAG:
-- ALTER TASK SKYPOINTS_TASK_05_GOLD_LOAD RESUME;
-- ALTER TASK SKYPOINTS_TASK_04_COUNTRY_SPLIT RESUME;
-- ALTER TASK SKYPOINTS_TASK_03_SILVER_TRANSFORM RESUME;
-- ALTER TASK SKYPOINTS_TASK_02_JSON_FLATTEN RESUME;
-- ALTER TASK SKYPOINTS_TASK_01_RAW_INGEST RESUME;
*/


;




