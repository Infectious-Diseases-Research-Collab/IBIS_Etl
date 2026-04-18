-- DROP+CREATE is safe here: this pipeline runs in batch mode with no concurrent
-- readers expected during ETL execution. The brief table-absence window between
-- DROP and CREATE is acceptable.
DROP TABLE IF EXISTS gold_ibis.baseline;
CREATE TABLE gold_ibis.baseline AS
SELECT * FROM (
    SELECT b.*
    FROM silver_ibis.baseline b
    WHERE uniqueid IS NOT NULL
) t
-- Exclude ETL pipeline tracking columns; all survey data columns are retained.
;

ALTER TABLE gold_ibis.baseline
    DROP COLUMN IF EXISTS run_uuid,
    DROP COLUMN IF EXISTS file_name,
    DROP COLUMN IF EXISTS file_path,
    DROP COLUMN IF EXISTS extracted_at;
