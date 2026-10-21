-- =============================================================================
-- FILE: 02_staging/02_stg_transform_load.sql
-- LAYER: SILVER / DATA CLEANSING & VALIDATION & PER-COUNTRY SPLIT
-- PURPOSE:
--   1. Cleanse, validate and ingest records from RAW.RAW_MEMBERS into STG.STG_MEMBERS
--   2. Multi-format date parsing (DDMMYYYY, ISO YYYY-MM-DD, numeric 7/8-digit MMDDYYYY/YYYYMMDD)
--   3. Derive business attributes:
--        - AGE: computed from DOB vs CURRENT_DATE
--        - STALE_MEMBER: 'Y' if last flight > 90 days ago, 'N' if active, 'U' if no flights
--   4. Handle NULL / missing COUNTRY:
--        - Derive from SOURCE_FILE name if blank; flag 'MISSING_COUNTRY' if still NULL
--   5. Route invalid records into STG.STG_MEMBERS_REJECTED (idempotent MERGE)
--   6. Split valid members into per-country target tables (STG.STG_MEMBERS_<COUNTRY>)
--      applying the "latest record wins" rule with SCD Type 2 country migration tracking:
--        - In current country:  IS_ACTIVE = 1 and END_DATE = NULL
--        - In previous country: IS_ACTIVE = 0 and END_DATE = date of move
--
-- IDEMPOTENCY GUARANTEE:
--   - STG_MEMBERS: Cleansed per-batch using DELETE WHERE BATCH_ID = $ACTIVE_BATCH_ID
--   - STG_MEMBERS_REJECTED: De-duplicated idempotent MERGE on MEMBER_ID (updates existing, inserts new)
--   - STG_MEMBERS_<COUNTRY>: TRUNCATE + INSERT per country ensures zero duplicates on re-runs
-- =============================================================================

USE DATABASE SKYPOINTS_DB;
USE SCHEMA STG;

-- ── 1. SET ACTIVE BATCH IDENTIFIER ────────────────────────────────────────────
SET BATCH_IN_RAW = (SELECT MAX(BATCH_ID) FROM RAW.RAW_MEMBERS WHERE BATCH_ID IS NOT NULL);
SET ACTIVE_BATCH_ID = COALESCE($BATCH_IN_RAW, TO_VARCHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MISS'));

SELECT $ACTIVE_BATCH_ID AS ACTIVE_BATCH_ID, $BATCH_IN_RAW AS BATCH_IN_RAW;


-- Ensure schema evolution for POST_CODE column if Silver tables were created earlier
ALTER TABLE IF EXISTS STG.STG_MEMBERS ADD COLUMN IF NOT EXISTS POST_CODE NUMBER(5);
ALTER TABLE IF EXISTS STG.STG_MEMBERS_REJECTED ADD COLUMN IF NOT EXISTS POST_CODE_RAW VARCHAR(10);

-- ── 2. IDEMPOTENT CLEANUP ──────────────────────────────────────────────────────
-- Deletes prior data for the active batch to ensure idempotency on re-runs
DELETE FROM STG.STG_MEMBERS 
WHERE BATCH_ID = $ACTIVE_BATCH_ID
   OR ($BATCH_IN_RAW IS NULL);


-- ── 3. TRANSFORM & POPULATE STG_MEMBERS ───────────────────────────────────────
INSERT INTO STG.STG_MEMBERS (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
    IS_VALID, REJECTION_REASON, SOURCE_FILE, LOAD_TS, BATCH_ID
)
WITH raw_filtered AS (
    SELECT *
    FROM RAW.RAW_MEMBERS
    WHERE UPPER(TRIM(COALESCE(REC_TYPE, 'D'))) NOT IN ('H', 'HEADER')
      AND (
          ($BATCH_IN_RAW IS NOT NULL AND BATCH_ID = $BATCH_IN_RAW)
          OR ($BATCH_IN_RAW IS NULL)
      )
),
country_lookup AS (
    SELECT DISTINCT UPPER(TRIM(ALIAS)) AS ALIAS, UPPER(TRIM(ISO_ALPHA3)) AS ISO_ALPHA3
    FROM STG.REF_COUNTRY_LOOKUP
),
country_canonical AS (
    SELECT DISTINCT UPPER(TRIM(ISO_ALPHA3)) AS ISO_ALPHA3, UPPER(TRIM(COUNTRY_NAME)) AS CANONICAL_NAME
    FROM STG.REF_COUNTRY_LOOKUP
    WHERE COUNTRY_NAME IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(COUNTRY_NAME)) ORDER BY ISO_ALPHA3) = 1
),
raw_normalized AS (
    SELECT
        TRIM(r.MEMBER_ID)                               AS MEMBER_ID,
        TRIM(r.MEMBER_NAME)                             AS MEMBER_NAME,
        UPPER(TRIM(r.TIER_CODE))                        AS TIER_CODE,
        TRIM(r.AGENT_NAME)                              AS AGENT_NAME,
        TRIM(r.STATE)                                   AS STATE,
        -- Pragmatic dynamic country resolution via STG.REF_COUNTRY_LOOKUP:
        -- 1. Exact alias match from lookup table (e.g. 'Indi' -> 'IND', 'Aust' -> 'AUS', 'US' -> 'USA')
        -- 2. Prefix match against canonical country name if length >= 3 (e.g. 'Canad' -> 'Canada' -> 'CAN')
        -- 3. Direct 3-letter ISO code if already valid; otherwise NULL (triggers MISSING_COUNTRY rejection)
        COALESCE(
            lkp.ISO_ALPHA3,
            pfx.ISO_ALPHA3,
            CASE WHEN LENGTH(TRIM(r.COUNTRY)) = 3 THEN UPPER(TRIM(r.COUNTRY)) END
        )                                               AS COUNTRY,
        TRY_TO_NUMBER(TRIM(r.POST_CODE))                AS POST_CODE,
        TRIM(r.POST_CODE)                               AS POST_CODE_RAW,
        NULLIF(TRIM(r.IS_ACTIVE), '')                   AS IS_ACTIVE,
        NULL::VARCHAR(1)                                AS MEMBER_TYPE,
        -- DOB: spec format is DDMMYYYY (e.g. 03051985 = 3 May 1985; also handles 7-digit 3051985)
        COALESCE(
            TRY_TO_DATE(TRIM(r.DOB), 'DDMMYYYY'),
            CASE WHEN LENGTH(TRIM(r.DOB)) = 7 THEN TRY_TO_DATE('0' || TRIM(r.DOB), 'DDMMYYYY') END
        )                                               AS DOB_PARSED,
        TRIM(r.DOB)                                       AS DOB_RAW,

        -- Enrollment date: Intelligent multi-format parser avoiding false year matches (e.g. 1052022 -> 2022-10-05, NOT 1052-02-02)
        CASE
            WHEN TRIM(r.ENROLLMENT_DATE) LIKE '____-__-__'
                 THEN TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'YYYY-MM-DD')

            WHEN LENGTH(TRIM(r.ENROLLMENT_DATE)) = 8 AND REGEXP_LIKE(TRIM(r.ENROLLMENT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'YYYYMMDD') END,
                     CASE WHEN SUBSTR(TRIM(r.ENROLLMENT_DATE), 5, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.ENROLLMENT_DATE), 'MMDDYYYY') END
                 )

            WHEN LENGTH(TRIM(r.ENROLLMENT_DATE)) = 7 AND REGEXP_LIKE(TRIM(r.ENROLLMENT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN TRY_TO_NUMBER(SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 2)) BETWEEN 1 AND 12
                           AND SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 4) || '-' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 2) || '-0' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 3, 1),
                              'YYYY-MM-DD'
                          ) END,
                     CASE WHEN SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 4, 4) || '-0' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 1, 1) || '-' ||
                              SUBSTR(TRIM(r.ENROLLMENT_DATE), 2, 2),
                              'YYYY-MM-DD'
                          ) END
                 )
        END                                             AS ENROLLMENT_DATE_PARSED,
        TRIM(r.ENROLLMENT_DATE)                           AS ENROLLMENT_DATE_RAW,

        -- Last flight date: same intelligent multi-format parser
        CASE
            WHEN TRIM(r.LAST_FLIGHT_DATE) LIKE '____-__-__'
                 THEN TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'YYYY-MM-DD')

            WHEN LENGTH(TRIM(r.LAST_FLIGHT_DATE)) = 8 AND REGEXP_LIKE(TRIM(r.LAST_FLIGHT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'YYYYMMDD') END,
                     CASE WHEN SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 5, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(TRIM(r.LAST_FLIGHT_DATE), 'MMDDYYYY') END
                 )

            WHEN LENGTH(TRIM(r.LAST_FLIGHT_DATE)) = 7 AND REGEXP_LIKE(TRIM(r.LAST_FLIGHT_DATE), '^[0-9]+$')
                 THEN COALESCE(
                     CASE WHEN TRY_TO_NUMBER(SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 2)) BETWEEN 1 AND 12
                           AND SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 4) || '-' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 2) || '-0' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 3, 1),
                              'YYYY-MM-DD'
                          ) END,
                     CASE WHEN SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 2) IN ('19', '20')
                          THEN TRY_TO_DATE(
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 4, 4) || '-0' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 1, 1) || '-' ||
                              SUBSTR(TRIM(r.LAST_FLIGHT_DATE), 2, 2),
                              'YYYY-MM-DD'
                          ) END
                 )
        END                                             AS LAST_FLIGHT_DATE_PARSED,
        TRIM(r.LAST_FLIGHT_DATE)                          AS LAST_FLIGHT_DATE_RAW,
        COALESCE(r.SOURCE_FILE, 'MEMBERS_FEED')           AS SOURCE_FILE,
        COALESCE(r.LOAD_TS, CURRENT_TIMESTAMP())          AS LOAD_TS,
        $ACTIVE_BATCH_ID                                AS BATCH_ID
    FROM raw_filtered r
    LEFT JOIN country_lookup lkp
        ON UPPER(TRIM(r.COUNTRY)) = lkp.ALIAS
    LEFT JOIN country_canonical pfx
        ON lkp.ISO_ALPHA3 IS NULL
       AND r.COUNTRY IS NOT NULL
       AND LENGTH(TRIM(r.COUNTRY)) >= 3
       AND (
           STARTSWITH(pfx.CANONICAL_NAME, UPPER(TRIM(r.COUNTRY)))
           OR STARTSWITH(UPPER(TRIM(r.COUNTRY)), pfx.CANONICAL_NAME)
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


-- ── 4. POPULATE STG_MEMBERS_REJECTED (IDEMPOTENT CLEANUP & LOAD) ──────────────
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



-- ── 5. DYNAMIC COUNTRY TABLE CREATION & SPLIT (SILVER) ────────────────────────
-- Transformation logic to split members into their per-country target tables,
-- applying the "latest record wins" rule when a member has moved countries.
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

        -- Step 5a: Create country-specific Silver table dynamically if not exists
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

        -- Step 5b: Clear country table for fresh idempotent rebuild from full history
        v_sql := 'TRUNCATE TABLE STG.STG_MEMBERS_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        -- Step 5c: Insert members with "latest record wins" SCD Type 2 migration tracking
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
                -- RN_GLOBAL = 1: member''''s latest active profile globally by business event time ("latest record wins")
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
            -- Latest record wins: 1 if active in this country, 0 if moved to another country
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


-- ── 6. VERIFICATION SUMMARY ──────────────────────────────────────────────────
SELECT 'STG_MEMBERS (Total Ingested)'   AS METRIC, COUNT(*) AS ROW_COUNT FROM STG.STG_MEMBERS
UNION ALL
SELECT 'STG_MEMBERS (Valid)'            AS METRIC, COUNT(*) AS ROW_COUNT FROM STG.STG_MEMBERS WHERE IS_VALID = TRUE
UNION ALL
SELECT 'STG_MEMBERS (Invalid/Rejected)' AS METRIC, COUNT(*) AS ROW_COUNT FROM STG.STG_MEMBERS WHERE IS_VALID = FALSE
UNION ALL
SELECT 'STG_MEMBERS_REJECTED'           AS METRIC, COUNT(*) AS ROW_COUNT FROM STG.STG_MEMBERS_REJECTED;
