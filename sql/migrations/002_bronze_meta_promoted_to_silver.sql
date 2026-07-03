-- Migration 002: track which bronze_ibis.meta rows have had their data
-- folded into silver_ibis (see modules/incremental_writer.py). NULL means
-- not yet promoted. Safe to run repeatedly.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'bronze_ibis' AND table_name = 'meta'
    ) THEN
        ALTER TABLE bronze_ibis.meta
            ADD COLUMN IF NOT EXISTS promoted_to_silver_at TIMESTAMP;
    END IF;
END $$;
