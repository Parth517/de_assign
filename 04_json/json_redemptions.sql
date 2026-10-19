-- =============================================================================
-- FILE: 04_json/json_redemptions.sql
-- LAYER: SILVER / ZERO-STORAGE JSON FLATTENING VIEW
-- PURPOSE:
--   - Query semi-structured JSON directly from RAW.RAW_REDEMPTIONS
--   - Provide a zero-copy, zero-storage virtual view (VW_REDEMPTIONS)
--   - No physical table duplication in STG!
--   - Show direct queries and joins back to member profiles
-- =============================================================================

USE DATABASE SKYPOINTS_DB;
USE SCHEMA STG;

-- ── 1. CLEANUP OLD PHYSICAL TABLE (IF PREVIOUSLY CREATED) ────────────────────
-- Drops any physical table so zero duplicated storage is used
DROP TABLE IF EXISTS STG.STG_REDEMPTIONS;


-- ── 2. ZERO-STORAGE FLATTENED VIEW ───────────────────────────────────────────
-- Flattens JSON on-the-fly directly from RAW.RAW_REDEMPTIONS using LATERAL FLATTEN.
-- Zero ETL overhead, zero storage duplication, always 100% in sync with RAW.
CREATE OR REPLACE VIEW STG.VW_REDEMPTIONS AS
SELECT
    m.VALUE:member_id::VARCHAR(20)                               AS MEMBER_ID,
    TRY_TO_DATE(m.VALUE:feed_date::VARCHAR, 'YYYYMMDD')          AS FEED_DATE,
    f.VALUE:txn_id::VARCHAR(50)                                  AS TXN_ID,
    TRY_TO_DATE(f.VALUE:txn_date::VARCHAR, 'YYYYMMDD')           AS TXN_DATE,
    f.VALUE:partner::VARCHAR(100)                                AS PARTNER,
    f.VALUE:miles_redeemed::NUMBER(12)                           AS MILES_REDEEMED,
    f.VALUE:status::VARCHAR(20)                                  AS STATUS,
    r.SOURCE_FILE,
    r.LOAD_TS,
    r.BATCH_ID
FROM RAW.RAW_REDEMPTIONS r,
     LATERAL FLATTEN(
         INPUT => CASE 
             WHEN IS_ARRAY(r.JSON_PAYLOAD) THEN r.JSON_PAYLOAD 
             ELSE ARRAY_CONSTRUCT(r.JSON_PAYLOAD) 
         END
     ) m,
     LATERAL FLATTEN(INPUT => m.VALUE:redemptions) f;

-- View alias so existing queries remain fully compatible without table duplication
CREATE OR REPLACE VIEW STG.STG_REDEMPTIONS AS
SELECT * FROM STG.VW_REDEMPTIONS;


-- ── 3. DIRECT RAW JSON QUERY (NO STAGING VIEW OR TABLE NEEDED) ───────────────
-- Direct ad-hoc query on RAW.RAW_REDEMPTIONS VARIANT column
SELECT
    m.VALUE:member_id::VARCHAR(20)                               AS MEMBER_ID,
    TRY_TO_DATE(m.VALUE:feed_date::VARCHAR, 'YYYYMMDD')          AS FEED_DATE,
    f.VALUE:txn_id::VARCHAR(50)                                  AS TXN_ID,
    TRY_TO_DATE(f.VALUE:txn_date::VARCHAR, 'YYYYMMDD')           AS TXN_DATE,
    f.VALUE:partner::VARCHAR(100)                                AS PARTNER,
    f.VALUE:miles_redeemed::NUMBER(12)                           AS MILES_REDEEMED,
    f.VALUE:status::VARCHAR(20)                                  AS STATUS
FROM RAW.RAW_REDEMPTIONS r,
     LATERAL FLATTEN(
         INPUT => CASE 
             WHEN IS_ARRAY(r.JSON_PAYLOAD) THEN r.JSON_PAYLOAD 
             ELSE ARRAY_CONSTRUCT(r.JSON_PAYLOAD) 
         END
     ) m,
     LATERAL FLATTEN(INPUT => m.VALUE:redemptions) f;


-- ── 4. JOINING REDEMPTIONS TO MEMBER PROFILES ────────────────────────────────

-- 4a. Basic join: get member name + tier alongside each transaction
SELECT
    m.MEMBER_ID,
    m.MEMBER_NAME,
    m.TIER_CODE,
    m.COUNTRY,
    r.TXN_ID,
    r.TXN_DATE,
    r.PARTNER,
    r.MILES_REDEEMED,
    r.STATUS
FROM STG.STG_MEMBERS   m
JOIN STG.VW_REDEMPTIONS r
  ON m.MEMBER_ID = r.MEMBER_ID
WHERE m.IS_VALID = TRUE
ORDER BY m.MEMBER_ID, r.TXN_DATE;


-- 4b. Aggregated view: total miles redeemed per member, per status
SELECT
    m.MEMBER_ID,
    m.MEMBER_NAME,
    m.TIER_CODE,
    m.COUNTRY,
    r.STATUS,
    COUNT(r.TXN_ID)         AS TRANSACTION_COUNT,
    SUM(r.MILES_REDEEMED)   AS TOTAL_MILES_REDEEMED
FROM STG.STG_MEMBERS   m
JOIN STG.VW_REDEMPTIONS r
  ON m.MEMBER_ID = r.MEMBER_ID
WHERE m.IS_VALID = TRUE
GROUP BY m.MEMBER_ID, m.MEMBER_NAME, m.TIER_CODE, m.COUNTRY, r.STATUS
ORDER BY TOTAL_MILES_REDEEMED DESC;


-- 4c. Members with ONLY PENDING redemptions (fraud / delay flag)
SELECT
    m.MEMBER_ID,
    m.MEMBER_NAME,
    m.COUNTRY,
    COUNT(r.TXN_ID)         AS PENDING_COUNT,
    SUM(r.MILES_REDEEMED)   AS PENDING_MILES
FROM STG.STG_MEMBERS   m
JOIN STG.VW_REDEMPTIONS r ON m.MEMBER_ID = r.MEMBER_ID
WHERE r.STATUS = 'PENDING'
  AND m.IS_VALID = TRUE
GROUP BY m.MEMBER_ID, m.MEMBER_NAME, m.COUNTRY
HAVING COUNT(r.TXN_ID) = COUNT(*);


-- ── 5. VERIFICATION SUMMARY ──────────────────────────────────────────────────
SELECT 'RAW.RAW_REDEMPTIONS (Payload Rows in RAW)' AS METRIC, COUNT(*) AS ROW_COUNT 
FROM RAW.RAW_REDEMPTIONS
UNION ALL
SELECT 'STG.VW_REDEMPTIONS (Virtual Flattened Rows)' AS METRIC, COUNT(*) AS ROW_COUNT 
FROM STG.VW_REDEMPTIONS;
