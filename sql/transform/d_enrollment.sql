-- DROP+CREATE is safe here: this pipeline runs in batch mode with no concurrent
-- readers expected during ETL execution. The brief table-absence window between
-- DROP and CREATE is acceptable.
DROP TABLE IF EXISTS gold_ibis.d_enrollment;
CREATE TABLE gold_ibis.d_enrollment AS
SELECT
    uniqueid,
    subjid,
    screening_id,
    countrycode,
    tabletnum,
    health_facility,
    consent,
    vdate,
    starttime,
    stoptime,
    interviewer_id
FROM silver_ibis.baseline
WHERE consent::integer = 1
  AND subjid IS NOT NULL;
