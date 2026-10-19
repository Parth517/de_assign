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
--   3. Latest Record Wins Rule:
--      When a member moves countries (e.g. IND -> USA), ROW_NUMBER OVER LOAD_TS DESC
--      picks their current country. They are routed ONLY to that country's table.
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
            IS_ACTIVE           VARCHAR(1),
            MEMBER_TYPE         VARCHAR(1),
            SOURCE_FILE         VARCHAR(500),
            BATCH_ID            VARCHAR(50),
            STG_LOAD_TS         TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP()
        ) CLUSTER BY (MEMBER_ID)
        COMMENT = ''Silver: current state of members for country ' || v_country || ' (created dynamically)''';
        EXECUTE IMMEDIATE v_sql;

        -- Step 2: Clear country table for fresh idempotent rebuild from full history
        v_sql := 'TRUNCATE TABLE STG.STG_MEMBERS_' || v_country;
        EXECUTE IMMEDIATE v_sql;

        -- Step 3: Insert members whose latest valid profile belongs to this country
        v_sql := 'INSERT INTO STG.STG_MEMBERS_' || v_country || ' (
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
            SOURCE_FILE, BATCH_ID
        )
        WITH latest_valid_members AS (
            SELECT
                MEMBER_ID,
                MEMBER_NAME,
                TIER_CODE,
                DOB,
                ENROLLMENT_DATE,
                LAST_FLIGHT_DATE,
                -- Fresh AGE computation
                CASE WHEN DOB IS NOT NULL THEN DATEDIFF(''year'', DOB, CURRENT_DATE()) END AS AGE,
                -- Fresh STALE_MEMBER computation (last flight > 90 days)
                CASE
                    WHEN LAST_FLIGHT_DATE IS NULL                                THEN ''U''
                    WHEN DATEDIFF(''day'', LAST_FLIGHT_DATE, CURRENT_DATE()) > 90 THEN ''Y''
                    ELSE                                                           ''N''
                END AS STALE_MEMBER,
                AGENT_NAME,
                STATE,
                COUNTRY,
                IS_ACTIVE,
                MEMBER_TYPE,
                SOURCE_FILE,
                BATCH_ID,
                ROW_NUMBER() OVER (
                    PARTITION BY MEMBER_ID
                    ORDER BY LOAD_TS DESC       -- latest record across all history wins
                ) AS RN
            FROM STG.STG_MEMBERS
            WHERE IS_VALID = TRUE
        )
        SELECT
            MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
            AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, IS_ACTIVE, MEMBER_TYPE,
            SOURCE_FILE, BATCH_ID
        FROM latest_valid_members
        WHERE RN = 1 AND COUNTRY = ''' || v_country || '''';
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
