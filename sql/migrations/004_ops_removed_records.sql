-- Tombstone ledger for modules/stale_records.py::remove_stale_records.
-- Records intent (what was removed, from where, when, why) for records
-- deleted from silver_ibis.<table> after being confirmed absent from a
-- tablet's recent syncs. Does NOT duplicate the row's data — that already
-- lives permanently in bronze_ibis and silver_ibis.<table>_history (both
-- append-only), so this only needs to record that a removal happened.
CREATE TABLE IF NOT EXISTS ops.removed_records (
    id         SERIAL PRIMARY KEY,
    table_name TEXT NOT NULL,
    uniqueid   TEXT NOT NULL,
    subjid     TEXT,
    tabletnum  TEXT,
    reason     TEXT NOT NULL,
    removed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
