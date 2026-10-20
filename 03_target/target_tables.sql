-- =============================================================================
-- FILE: 03_targets/target_tables.sql
-- LAYER: GOLD / PRODUCTION
-- PURPOSE:
--   Dead-simple load from Silver into Gold.
--   ZERO business logic here.
--   Silver (staging.sql) has already done:
--     - Date parsing and type casting
--     - AGE and STALE_MEMBER computation
--     - IS_VALID validation and rejection
--     - Latest-record-wins deduplication (1 row per MEMBER_ID)
--     - Country-level split (STG_MEMBERS_USA / IND / AUS)
--
--   This file only does:
--     1. DDL for the 3 Gold target tables
--     2. TRUNCATE + INSERT (full refresh, no MERGE, no SP, no logic)
--
-- SCALABILITY:
--   - CLUSTER BY (MEMBER_ID) for fast point lookups
--   - Full refresh is safe because Silver already guarantees 1 row per member
--   - Use a LARGE warehouse for the INSERT when processing billions of records
-- =============================================================================

USE DATABASE SKYPOINTS_DB;
USE SCHEMA PROD;


-- =============================================================================
-- 1. GOLD TABLE DDL
-- =============================================================================

CREATE OR REPLACE TABLE PROD.TABLE_USA (
    MEMBER_ID               VARCHAR(20)     NOT NULL,
    MEMBER_NAME             VARCHAR(100),
    TIER_CODE               VARCHAR(10),
    DOB                     DATE,
    ENROLLMENT_DATE         DATE,
    LAST_FLIGHT_DATE        DATE,
    AGE                     NUMBER(3),
    STALE_MEMBER            VARCHAR(1),
    AGENT_NAME              VARCHAR(100),
    STATE                   VARCHAR(50),
    COUNTRY                 VARCHAR(10),
    IS_ACTIVE               NUMBER(1)       COMMENT '1 if currently active in USA, 0 if moved to another country',
    MEMBER_TYPE             VARCHAR(1),
    START_DATE              DATE            COMMENT 'Date member became active in USA',
    END_DATE                DATE            COMMENT 'Date member moved away; NULL if currently active',
    SOURCE_FILE             VARCHAR(500),
    BATCH_ID                VARCHAR(50),
    LOADED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID)
COMMENT = 'Gold: USA members with migration tracking (1=active, 0=moved).';


CREATE OR REPLACE TABLE PROD.TABLE_IND (
    MEMBER_ID               VARCHAR(20)     NOT NULL,
    MEMBER_NAME             VARCHAR(100),
    TIER_CODE               VARCHAR(10),
    DOB                     DATE,
    ENROLLMENT_DATE         DATE,
    LAST_FLIGHT_DATE        DATE,
    AGE                     NUMBER(3),
    STALE_MEMBER            VARCHAR(1),
    AGENT_NAME              VARCHAR(100),
    STATE                   VARCHAR(50),
    COUNTRY                 VARCHAR(10),
    IS_ACTIVE               NUMBER(1)       COMMENT '1 if currently active in India, 0 if moved to another country',
    MEMBER_TYPE             VARCHAR(1)      COMMENT 'I=Individual C=Corporate',
    START_DATE              DATE            COMMENT 'Date member became active in India',
    END_DATE                DATE            COMMENT 'Date member moved away; NULL if currently active',
    SOURCE_FILE             VARCHAR(500),
    BATCH_ID                VARCHAR(50),
    LOADED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID)
COMMENT = 'Gold: India members with migration tracking (1=active, 0=moved).';


CREATE OR REPLACE TABLE PROD.TABLE_AUS (
    MEMBER_ID               VARCHAR(20)     NOT NULL,
    MEMBER_NAME             VARCHAR(100),
    TIER_CODE               VARCHAR(10),
    DOB                     DATE,
    ENROLLMENT_DATE         DATE,
    LAST_FLIGHT_DATE        DATE,
    AGE                     NUMBER(3),
    STALE_MEMBER            VARCHAR(1),
    AGENT_NAME              VARCHAR(100),
    STATE                   VARCHAR(50),
    COUNTRY                 VARCHAR(10),
    IS_ACTIVE               NUMBER(1)       COMMENT '1 if currently active in Australia, 0 if moved to another country',
    MEMBER_TYPE             VARCHAR(1),
    START_DATE              DATE            COMMENT 'Date member became active in Australia',
    END_DATE                DATE            COMMENT 'Date member moved away; NULL if currently active',
    SOURCE_FILE             VARCHAR(500),
    BATCH_ID                VARCHAR(50),
    LOADED_AT               TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (MEMBER_ID)
COMMENT = 'Gold: Australia members with migration tracking (1=active, 0=moved).';


-- =============================================================================
-- 2. LOAD GOLD — TRUNCATE + INSERT (Full Refresh, No Logic)
--
--    Silver guarantees:
--      - Only IS_VALID = TRUE rows are here
--      - Exactly 1 row per MEMBER_ID (latest record wins already applied)
--      - Dates are typed, AGE and STALE_MEMBER are computed
--      - Country split is already done
--
--    So this is literally just: clear the table and copy from Silver.
-- =============================================================================

-- ── USA ──────────────────────────────────────────────────────────────────────
TRUNCATE TABLE PROD.TABLE_USA;

INSERT INTO PROD.TABLE_USA (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
)
SELECT
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
FROM STG.STG_MEMBERS_USA
WHERE ENROLLMENT_DATE IS NOT NULL
  AND ENROLLMENT_DATE <= CURRENT_DATE()
  AND (LAST_FLIGHT_DATE IS NULL OR ENROLLMENT_DATE <= LAST_FLIGHT_DATE)
  AND (DOB IS NULL OR (DOB <= CURRENT_DATE() AND DATEDIFF('year', DOB, CURRENT_DATE()) BETWEEN 0 AND 120))
  AND MEMBER_ID NOT IN ('', '0', '00000000');


-- ── IND ──────────────────────────────────────────────────────────────────────
TRUNCATE TABLE PROD.TABLE_IND;

INSERT INTO PROD.TABLE_IND (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
)
SELECT
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
FROM STG.STG_MEMBERS_IND
WHERE ENROLLMENT_DATE IS NOT NULL
  AND ENROLLMENT_DATE <= CURRENT_DATE()
  AND (LAST_FLIGHT_DATE IS NULL OR ENROLLMENT_DATE <= LAST_FLIGHT_DATE)
  AND (DOB IS NULL OR (DOB <= CURRENT_DATE() AND DATEDIFF('year', DOB, CURRENT_DATE()) BETWEEN 0 AND 120))
  AND MEMBER_ID NOT IN ('', '0', '00000000');


-- ── AUS ──────────────────────────────────────────────────────────────────────
TRUNCATE TABLE PROD.TABLE_AUS;

INSERT INTO PROD.TABLE_AUS (
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
)
SELECT
    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
FROM STG.STG_MEMBERS_AUS
WHERE ENROLLMENT_DATE IS NOT NULL
  AND ENROLLMENT_DATE <= CURRENT_DATE()
  AND (LAST_FLIGHT_DATE IS NULL OR ENROLLMENT_DATE <= LAST_FLIGHT_DATE)
  AND (DOB IS NULL OR (DOB <= CURRENT_DATE() AND DATEDIFF('year', DOB, CURRENT_DATE()) BETWEEN 0 AND 120))
  AND MEMBER_ID NOT IN ('', '0', '00000000');


-- =============================================================================
-- 3. DYNAMIC PER-COUNTRY TABLE GENERATION (Scale to ALL Countries)
-- =============================================================================
-- Problem: The assessment specifies:
--   "All members related to India will go to Table_India, and so on for each country."
--
-- In reality, SkyPoints receives members from Canada (CAN), Philippines (PHIL),
-- and potentially dozens of other countries. Manually creating 100+ physical DDLs
-- is unmaintainable.
--
-- Solution: This Stored Procedure queries all distinct countries present in
-- STG.STG_MEMBERS, dynamically creates PROD.TABLE_<COUNTRY> if it does not
-- already exist, and populates it with that country's latest records.
--
-- New countries are provisioned automatically without touching code!
-- =============================================================================

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

        -- Step 1: Create target table dynamically if it does not exist
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

        -- Step 2: Clear existing records in this country table for fresh refresh
        v_sql := 'TRUNCATE TABLE PROD.TABLE_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        -- Step 3: Direct copy from Silver country table (ZERO processing in Gold)
        v_sql := 'INSERT INTO PROD.TABLE_' || v_country || ' (
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
            START_DATE, END_DATE, SOURCE_FILE, BATCH_ID
        )
        SELECT
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
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


-- =============================================================================
-- 4. EXECUTE & VERIFY
-- =============================================================================

-- Run the dynamic procedure (creates and populates TABLE_USA, TABLE_IND, TABLE_AUS, TABLE_CAN, TABLE_PHIL, etc.):
CALL PROD.SP_LOAD_DYNAMIC_COUNTRY_TABLES();

-- View all country tables created dynamically in PROD:
SHOW TABLES LIKE 'TABLE_%' IN SCHEMA PROD;

