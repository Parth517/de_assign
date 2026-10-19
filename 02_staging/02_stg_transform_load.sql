-- =============================================================================
-- FILE: 02_staging/02_stg_transform_load.sql
-- LAYER: SILVER / DATA CLEANSING & VALIDATION
-- PURPOSE:
--   1. Ingest current batch from RAW.RAW_MEMBERS into STG.STG_MEMBERS
--   2. Multi-format date parsing (YYYYMMDD, ISO YYYY-MM-DD, USA integer MMDDYYYY)
--   3. Derive business attributes:
--        - AGE: computed from DOB vs CURRENT_DATE
--        - STALE_MEMBER: flag 'Y' if last flight > 90 days ago, 'N' if active, 'U' if no flights
--   4. Handle NULL / missing COUNTRY:
--        - In the assessment spec, Country is NOT mandatory (Mandatory = N).
--        - When COUNTRY is blank/NULL, attempt derivation from SOURCE_FILE.
--        - If still NULL, flag with 'MISSING_COUNTRY' and route to REJECTED.
--   5. Route invalid records into STG.STG_MEMBERS_REJECTED for operational auditing.
-- =============================================================================

USE DATABASE SKYPOINTS_DB;
USE SCHEMA STG;

-- ── 1. SET ACTIVE BATCH IDENTIFIER (Optional session variable) ─────────────────
SET BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS);
SELECT $BATCH_ID AS ACTIVE_BATCH_ID;


-- ── 2. IDEMPOTENT CLEANUP FOR CURRENT BATCH ──────────────────────────────────
-- Removes existing rows for the latest batch only; prior batch history is preserved
DELETE FROM STG.STG_MEMBERS 
WHERE BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS);


-- ── 3. TRANSFORM & POPULATE STG_MEMBERS ───────────────────────────────────────
INSERT INTO STG.STG_MEMBERS (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    IS_VALID, REJECTION_REASON, SOURCE_FILE, LOAD_TS, BATCH_ID
)
WITH latest_batch AS (
    -- Automatically picks the most recent batch loaded in RAW
    SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') AS BATCH_ID
    FROM RAW.RAW_MEMBERS
),
raw_normalized AS (
    SELECT
        TRIM(r.MEMBER_ID)                               AS MEMBER_ID,
        TRIM(r.MEMBER_NAME)                             AS MEMBER_NAME,
        UPPER(TRIM(r.TIER_CODE))                        AS TIER_CODE,
        TRIM(r.AGENT_NAME)                              AS AGENT_NAME,
        TRIM(r.STATE)                                   AS STATE,

        -- Handle NULL Country:
        -- Source spec defines Country as optional (Mandatory = N).
        -- If blank/NULL, try deriving from SOURCE_FILE name; otherwise keep NULL.
        COALESCE(
            NULLIF(TRIM(r.COUNTRY), ''),
            CASE
                WHEN UPPER(r.SOURCE_FILE) LIKE '%IND%'   THEN 'IND'
                WHEN UPPER(r.SOURCE_FILE) LIKE '%USA%'   THEN 'USA'
                WHEN UPPER(r.SOURCE_FILE) LIKE '%AUS%'   THEN 'AUS'
                WHEN UPPER(r.SOURCE_FILE) LIKE '%CAN%'   THEN 'CAN'
                WHEN UPPER(r.SOURCE_FILE) LIKE '%PHIL%'  THEN 'PHIL'
                ELSE NULL
            END
        )                                               AS COUNTRY,

        NULLIF(TRIM(r.IS_ACTIVE), '')                   AS IS_ACTIVE,
        NULL::VARCHAR(1)                                AS MEMBER_TYPE,

        -- DOB: spec format is DDMMYYYY (e.g. 03051985 = 3 May 1985)
        TRY_TO_DATE(TRIM(r.DOB), 'DDMMYYYY')            AS DOB_PARSED,
        TRIM(r.DOB)                                     AS DOB_RAW,

        -- Enrollment date: try YYYYMMDD, then ISO, then USA integer MMDDYYYY
        COALESCE(
            TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'YYYYMMDD'),
            TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(
                SUBSTR(LPAD(TRIM(r.ENROLLMENT_DATE), 8, '0'), 5, 4) || '-' ||
                SUBSTR(LPAD(TRIM(r.ENROLLMENT_DATE), 8, '0'), 1, 2) || '-' ||
                SUBSTR(LPAD(TRIM(r.ENROLLMENT_DATE), 8, '0'), 3, 2),
                'YYYY-MM-DD'
            )
        )                                               AS ENROLLMENT_DATE_PARSED,
        TRIM(r.ENROLLMENT_DATE)                         AS ENROLLMENT_DATE_RAW,

        -- Last flight date: same multi-format parsing
        COALESCE(
            TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'YYYYMMDD'),
            TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'YYYY-MM-DD'),
            TRY_TO_DATE(
                SUBSTR(LPAD(TRIM(r.LAST_FLIGHT_DATE), 8, '0'), 5, 4) || '-' ||
                SUBSTR(LPAD(TRIM(r.LAST_FLIGHT_DATE), 8, '0'), 1, 2) || '-' ||
                SUBSTR(LPAD(TRIM(r.LAST_FLIGHT_DATE), 8, '0'), 3, 2),
                'YYYY-MM-DD'
            )
        )                                               AS LAST_FLIGHT_DATE_PARSED,
        TRIM(r.LAST_FLIGHT_DATE)                        AS LAST_FLIGHT_DATE_RAW,

        COALESCE(r.SOURCE_FILE, 'MEMBERS_FEED')         AS SOURCE_FILE,
        COALESCE(r.LOAD_TS, CURRENT_TIMESTAMP())        AS LOAD_TS,
        r.BATCH_ID                                      AS BATCH_ID

    FROM RAW.RAW_MEMBERS r
    JOIN latest_batch b
      ON r.BATCH_ID = b.BATCH_ID
    WHERE UPPER(TRIM(COALESCE(r.REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
),
with_derived AS (
    SELECT
        *,
        -- AGE computed from DOB
        CASE
            WHEN DOB_PARSED IS NOT NULL
            THEN DATEDIFF('year', DOB_PARSED, CURRENT_DATE())
        END                                             AS AGE,

        -- STALE_MEMBER flag: Y if last flight > 90 days ago
        CASE
            WHEN LAST_FLIGHT_DATE_PARSED IS NULL                                   THEN 'U'
            WHEN DATEDIFF('day', LAST_FLIGHT_DATE_PARSED, CURRENT_DATE()) > 90     THEN 'Y'
            ELSE                                                                        'N'
        END                                             AS STALE_MEMBER,

        -- Data Quality Validation Rules:
        TRIM(
            CONCAT_WS('|',
                CASE WHEN MEMBER_ID   IS NULL OR TRIM(MEMBER_ID)   = '' THEN 'MISSING_MEMBER_ID'          END,
                CASE WHEN MEMBER_NAME IS NULL OR TRIM(MEMBER_NAME) = '' THEN 'MISSING_MEMBER_NAME'        END,
                -- If COUNTRY is NULL/missing, flag it so it won't be routed to an orphaned table:
                CASE WHEN COUNTRY IS NULL OR TRIM(COUNTRY) IN ('', 'UNKNOWN') THEN 'MISSING_COUNTRY'     END,
                CASE WHEN TIER_CODE NOT IN ('PLT','GLD','SLV','BRZ')    THEN 'INVALID_TIER_CODE'          END,
                CASE WHEN IS_ACTIVE IS NULL                             THEN 'MISSING_IS_ACTIVE'          END,
                CASE WHEN DOB_RAW IS NOT NULL
                      AND DOB_PARSED IS NULL                            THEN 'INVALID_DOB_FORMAT'         END,
                CASE WHEN ENROLLMENT_DATE_RAW IS NOT NULL
                      AND ENROLLMENT_DATE_PARSED IS NULL               THEN 'INVALID_ENROLLMENT_DATE'     END,
                CASE WHEN LAST_FLIGHT_DATE_RAW IS NOT NULL
                      AND LAST_FLIGHT_DATE_PARSED IS NULL              THEN 'INVALID_FLIGHT_DATE'         END,
                CASE WHEN ENROLLMENT_DATE_PARSED IS NOT NULL
                      AND LAST_FLIGHT_DATE_PARSED IS NOT NULL
                      AND ENROLLMENT_DATE_PARSED > LAST_FLIGHT_DATE_PARSED
                                                                       THEN 'ENROLLMENT_AFTER_FLIGHT'     END,
                CASE WHEN DOB_PARSED IS NOT NULL
                      AND (DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) < 0
                        OR DATEDIFF('year', DOB_PARSED, CURRENT_DATE()) > 120)
                                                                       THEN 'UNREALISTIC_AGE'             END,
                CASE WHEN ENROLLMENT_DATE_PARSED > CURRENT_DATE()      THEN 'ENROLLMENT_DATE_IN_FUTURE'   END,
                CASE WHEN LAST_FLIGHT_DATE_PARSED > CURRENT_DATE()     THEN 'FLIGHT_DATE_IN_FUTURE'       END
            ),
            '|'
        )                                               AS REJECTION_REASON

    FROM raw_normalized
)
SELECT
    MEMBER_ID,
    MEMBER_NAME,
    TIER_CODE,
    DOB_PARSED              AS DOB,
    ENROLLMENT_DATE_PARSED  AS ENROLLMENT_DATE,
    LAST_FLIGHT_DATE_PARSED AS LAST_FLIGHT_DATE,
    AGE,
    STALE_MEMBER,
    AGENT_NAME,
    STATE,
    COUNTRY,
    IS_ACTIVE,
    MEMBER_TYPE,
    CASE WHEN REJECTION_REASON IS NULL OR REJECTION_REASON = '' THEN TRUE ELSE FALSE END AS IS_VALID,
    NULLIF(REJECTION_REASON, '')    AS REJECTION_REASON,
    SOURCE_FILE,
    LOAD_TS,
    BATCH_ID
FROM with_derived;


-- ── 4. POPULATE STG_MEMBERS_REJECTED ─────────────────────────────────────────
DELETE FROM STG.STG_MEMBERS_REJECTED 
WHERE BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS);

INSERT INTO STG.STG_MEMBERS_REJECTED (
    MEMBER_ID, MEMBER_NAME, TIER_CODE,
    DOB_RAW, ENROLLMENT_DATE_RAW, LAST_FLIGHT_DATE_RAW,
    COUNTRY, IS_ACTIVE, REJECTION_REASON,
    SOURCE_FILE, BATCH_ID
)
SELECT
    TRIM(r.MEMBER_ID),
    TRIM(r.MEMBER_NAME),
    UPPER(TRIM(r.TIER_CODE)),
    TRIM(r.DOB),
    TRIM(r.ENROLLMENT_DATE),
    TRIM(r.LAST_FLIGHT_DATE),
    NULLIF(TRIM(r.COUNTRY), ''),
    NULLIF(TRIM(r.IS_ACTIVE), ''),
    m.REJECTION_REASON,
    m.SOURCE_FILE,
    m.BATCH_ID
FROM RAW.RAW_MEMBERS r
JOIN STG.STG_MEMBERS m
  ON EQUAL_NULL(TRIM(r.MEMBER_ID), m.MEMBER_ID)
 AND EQUAL_NULL(TRIM(r.MEMBER_NAME), m.MEMBER_NAME)
 AND EQUAL_NULL(r.BATCH_ID, m.BATCH_ID)
WHERE UPPER(TRIM(COALESCE(r.REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
  AND m.BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS)
  AND m.IS_VALID = FALSE;


-- ── 5. VERIFICATION SUMMARY ──────────────────────────────────────────────────
-- Returns summary counts for the active batch in the Snowflake results pane
SELECT 'STG_MEMBERS (Total Ingested)' AS METRIC, COUNT(*) AS ROW_COUNT 
FROM STG.STG_MEMBERS 
WHERE BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS)

UNION ALL

SELECT 'STG_MEMBERS (Valid)' AS METRIC, COUNT(*) AS ROW_COUNT 
FROM STG.STG_MEMBERS 
WHERE BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS) AND IS_VALID = TRUE

UNION ALL

SELECT 'STG_MEMBERS (Invalid/Rejected)' AS METRIC, COUNT(*) AS ROW_COUNT 
FROM STG.STG_MEMBERS 
WHERE BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS) AND IS_VALID = FALSE

UNION ALL

SELECT 'STG_MEMBERS_REJECTED' AS METRIC, COUNT(*) AS ROW_COUNT 
FROM STG.STG_MEMBERS_REJECTED 
WHERE BATCH_ID = (SELECT COALESCE(MAX(BATCH_ID), 'DEFAULT_BATCH') FROM RAW.RAW_MEMBERS);
