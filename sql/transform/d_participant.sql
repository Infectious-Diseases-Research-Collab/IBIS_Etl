-- DROP+CREATE is safe here: this pipeline runs in batch mode with no concurrent
-- readers expected during ETL execution. The brief table-absence window between
-- DROP and CREATE is acceptable.
DROP TABLE IF EXISTS gold_ibis.d_participant;
CREATE TABLE gold_ibis.d_participant AS
SELECT
    uniqueid,
    screening_id,
    subjid,
    countrycode,
    tabletnum,
    client_sex,
    health_facility,
    consent,
    dob,
    age,
    mobile_number,
    participants_name,
    interviewer_id,
    starttime,
    vdate
FROM silver_ibis.baseline
WHERE uniqueid IS NOT NULL;
