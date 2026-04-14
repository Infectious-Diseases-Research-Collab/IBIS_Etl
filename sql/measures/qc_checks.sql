-- DROP+CREATE is safe here: this pipeline runs in batch mode with no concurrent
-- readers expected during ETL execution.
DROP TABLE IF EXISTS gold_ibis.ds_qc_summary;
CREATE TABLE gold_ibis.ds_qc_summary AS
SELECT
    countrycode,
    tabletnum,
    COUNT(*)                                                        AS total_records,
    COUNT(CASE WHEN consent::integer = 1 THEN 1 END)                        AS consented,
    COUNT(CASE WHEN subjid IS NOT NULL AND consent::integer = 1 THEN 1 END) AS enrolled,
    COUNT(CASE WHEN uniqueid IS NULL THEN 1 END)                             AS missing_uniqueid,
    COUNT(CASE WHEN screening_id IS NULL THEN 1 END)                         AS missing_screening_id,
    COUNT(CASE WHEN mobile_number IS NULL AND consent::integer = 1 THEN 1 END) AS missing_phone_consented
FROM silver_ibis.baseline
GROUP BY countrycode, tabletnum
ORDER BY countrycode, tabletnum;
