-- =============================================================================
-- FILE: 02_staging/03_dynamic_country_split.sql
-- LAYER: SILVER / DYNAMIC COUNTRY ROUTING
-- PURPOSE:
--   Dynamically creates and populates per-country Silver tables:
--     STG.STG_MEMBERS_<COUNTRY> (e.g. STG_MEMBERS_USA, STG_MEMBERS_IND,
--     STG_MEMBERS_AUS, STG_MEMBERS_CAN, STG_MEMBERS_PHIL, etc.)
--
-- KEY DESIGN PRINCIPLES:
--   1. Zero hardcoding of country codes:
--      Scans all distinct valid countries in STG.STG_MEMBERS.
--      Automatically creates new tables whenever new countries appear in the feed.
--   2. Handles NULL / Missing Country:
--      Strictly filters out NULL and 'UNKNOWN' countries so invalid records never
--      create malformed tables (those records remain safely in STG_MEMBERS_REJECTED).
--   3. Country Migration & SCD Type 2 Tracking (IS_ACTIVE & END_DATE):
--      When a member moves countries (e.g. IND -> USA):
--        - In the previous country (IND): IS_ACTIVE = 0 and END_DATE = date of move
--        - In the current country (USA):  IS_ACTIVE = 1 and END_DATE = NULL
--   4. Fresh Business Attributes:
--      AGE and STALE_MEMBER are recomputed fresh against CURRENT_DATE() during load.
-- =============================================================================

USE DATABASE SKYPOINTS_DB;
USE SCHEMA STG;

CREATE OR REPLACE PROCEDURE STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    -- Cursor for all valid countries present in history (strictly ignores NULL / UNKNOWN)
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

        -- Step 1: Create country-specific Silver table dynamically if not exists
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

        -- Step 2: Clear country table for fresh idempotent rebuild from full history
        v_sql := 'TRUNCATE TABLE STG.STG_MEMBERS_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        -- Step 3: Insert members: 1 in current country (END_DATE=NULL), 0 in previous country (END_DATE=move date)
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


-- =============================================================================
-- EXECUTE PROCEDURE & VERIFY TABLES
-- =============================================================================

-- Run the dynamic country split procedure:
CALL STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES();

-- List all country tables generated dynamically in STG:
SHOW TABLES LIKE 'STG_MEMBERS_%' IN SCHEMA STG;
