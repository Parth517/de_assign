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
    v_active_batch VARCHAR;
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
    -- Resolve active batch ID from session variable or fall back to latest in STG
    v_active_batch := COALESCE((SELECT $ACTIVE_BATCH_ID), (SELECT MAX(BATCH_ID) FROM STG.STG_MEMBERS));

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
            AGE                 NUMBER(3)       COMMENT ''Recomputed from DOB vs CURRENT_DATE'',
            STALE_MEMBER        VARCHAR(1)      COMMENT ''Recomputed from LAST_FLIGHT_DATE'',
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

        -- Step 2: Incremental SCD-2 Expiry (Close out members who moved AWAY from this country in the active batch)
        v_sql := 'UPDATE STG.STG_MEMBERS_' || v_country || ' tgt
            SET 
                tgt.IS_ACTIVE   = 0,
                tgt.END_DATE    = src.MOVE_DATE,
                tgt.STG_LOAD_TS = CURRENT_TIMESTAMP()
            FROM (
                SELECT 
                    s.MEMBER_ID,
                    COALESCE(s.LAST_FLIGHT_DATE, s.ENROLLMENT_DATE, CURRENT_DATE()) AS MOVE_DATE
                FROM STG.STG_MEMBERS s
                WHERE s.IS_VALID = TRUE
                  AND s.COUNTRY <> ''' || v_country || '''
                  AND (s.BATCH_ID = ''' || v_active_batch || ''' OR ''' || v_active_batch || ''' IS NULL)
            ) src
            WHERE tgt.MEMBER_ID = src.MEMBER_ID
              AND tgt.IS_ACTIVE = 1';
        EXECUTE IMMEDIATE v_sql;

        -- Step 3: Incremental MERGE with "Latest Country Wins" SCD Type 2 logic
        v_sql := 'MERGE INTO STG.STG_MEMBERS_' || v_country || ' tgt
            USING (
                WITH global_latest AS (
                    -- Identifies the single globally active country for each member
                    SELECT 
                        MEMBER_ID,
                        COUNTRY AS ACTIVE_COUNTRY,
                        COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) AS MOVE_DATE
                    FROM STG.STG_MEMBERS
                    WHERE IS_VALID = TRUE
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY MEMBER_ID 
                        ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
                    ) = 1
                ),
                country_records AS (
                    -- Latest record for this specific country
                    SELECT 
                        MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                        AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, MEMBER_TYPE,
                        COALESCE(ENROLLMENT_DATE, LOAD_TS::DATE) AS START_DATE,
                        SOURCE_FILE, BATCH_ID
                    FROM STG.STG_MEMBERS
                    WHERE IS_VALID = TRUE
                      AND COUNTRY = ''' || v_country || '''
                      AND (BATCH_ID = ''' || v_active_batch || ''' OR ''' || v_active_batch || ''' IS NULL)
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY MEMBER_ID 
                        ORDER BY COALESCE(LAST_FLIGHT_DATE, ENROLLMENT_DATE, LOAD_TS::DATE) DESC, LOAD_TS DESC
                    ) = 1
                )
                SELECT 
                    c.MEMBER_ID, c.MEMBER_NAME, c.TIER_CODE, c.DOB, c.ENROLLMENT_DATE, c.LAST_FLIGHT_DATE,
                    c.AGE, c.STALE_MEMBER, c.AGENT_NAME, c.STATE, c.COUNTRY, c.POST_CODE, c.MEMBER_TYPE,
                    c.START_DATE,
                    -- Latest Country Wins: 1 if this country is their latest profile globally; 0 if moved away
                    CASE WHEN g.ACTIVE_COUNTRY = ''' || v_country || ''' THEN 1 ELSE 0 END AS IS_ACTIVE,
                    CASE WHEN g.ACTIVE_COUNTRY = ''' || v_country || ''' THEN NULL ELSE g.MOVE_DATE END AS END_DATE,
                    c.SOURCE_FILE, c.BATCH_ID
                FROM country_records c
                JOIN global_latest g ON c.MEMBER_ID = g.MEMBER_ID
            ) src
            ON tgt.MEMBER_ID = src.MEMBER_ID
            WHEN MATCHED THEN
                UPDATE SET
                    tgt.MEMBER_NAME      = src.MEMBER_NAME,
                    tgt.TIER_CODE        = src.TIER_CODE,
                    tgt.DOB              = src.DOB,
                    tgt.ENROLLMENT_DATE  = src.ENROLLMENT_DATE,
                    tgt.LAST_FLIGHT_DATE = src.LAST_FLIGHT_DATE,
                    tgt.AGE              = src.AGE,
                    tgt.STALE_MEMBER     = src.STALE_MEMBER,
                    tgt.AGENT_NAME       = src.AGENT_NAME,
                    tgt.STATE            = src.STATE,
                    tgt.POST_CODE        = src.POST_CODE,
                    tgt.IS_ACTIVE        = src.IS_ACTIVE,
                    tgt.END_DATE         = src.END_DATE,
                    tgt.MEMBER_TYPE      = src.MEMBER_TYPE,
                    tgt.SOURCE_FILE      = src.SOURCE_FILE,
                    tgt.BATCH_ID         = src.BATCH_ID,
                    tgt.STG_LOAD_TS      = CURRENT_TIMESTAMP()
            WHEN NOT MATCHED THEN
                INSERT (
                    MEMBER_ID, MEMBER_NAME, TIER_CODE, DOB, ENROLLMENT_DATE, LAST_FLIGHT_DATE,
                    AGE, STALE_MEMBER, AGENT_NAME, STATE, COUNTRY, POST_CODE, IS_ACTIVE, MEMBER_TYPE,
                    START_DATE, END_DATE, SOURCE_FILE, BATCH_ID, STG_LOAD_TS
                )
                VALUES (
                    src.MEMBER_ID, src.MEMBER_NAME, src.TIER_CODE, src.DOB, src.ENROLLMENT_DATE, src.LAST_FLIGHT_DATE,
                    src.AGE, src.STALE_MEMBER, src.AGENT_NAME, src.STATE, src.COUNTRY, src.POST_CODE,
                    src.IS_ACTIVE, src.MEMBER_TYPE, src.START_DATE, src.END_DATE, src.SOURCE_FILE, src.BATCH_ID, CURRENT_TIMESTAMP()
                )';
        EXECUTE IMMEDIATE v_sql;

        v_count := v_count + 1;
    END FOR;

    RETURN 'Success: Incrementally synchronized ' || v_count || ' per-country tables in STG for batch ' || COALESCE(v_active_batch, 'ALL') || '.';
END;
$$;


-- =============================================================================
-- EXECUTE PROCEDURE & VERIFY TABLES
-- =============================================================================

-- Run the dynamic country split procedure:
CALL STG.SP_CREATE_AND_LOAD_COUNTRY_TABLES();


