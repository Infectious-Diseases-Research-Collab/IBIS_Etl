-- Run once to set up SMS tables. Safe to re-run (IF NOT EXISTS).
-- Schema 'sms' is created automatically by db.py init_schemas().

CREATE TABLE IF NOT EXISTS sms.templates (
    id              SERIAL PRIMARY KEY,
    arm             TEXT    NOT NULL,
    language        TEXT    NOT NULL,
    week            INTEGER NOT NULL,
    message_text    TEXT    NOT NULL,
    has_placeholder BOOLEAN DEFAULT FALSE,
    UNIQUE (arm, language, week)
);

CREATE TABLE IF NOT EXISTS sms.queue (
    id               SERIAL PRIMARY KEY,
    subjid           TEXT    NOT NULL,
    mobile_number    TEXT    NOT NULL,
    arm_text         TEXT    NOT NULL,
    language         TEXT    NOT NULL,
    week             INTEGER NOT NULL,
    scheduled_date   DATE    NOT NULL,
    appointment_date DATE,
    opted_out        BOOLEAN DEFAULT FALSE,
    status           TEXT    NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'sent', 'failed', 'skipped')),
    created_at       TIMESTAMP DEFAULT NOW(),
    UNIQUE (subjid, week)
);

CREATE TABLE IF NOT EXISTS sms.log (
    id                  SERIAL PRIMARY KEY,
    queue_id            INTEGER REFERENCES sms.queue(id),
    subjid              TEXT    NOT NULL,
    mobile_number       TEXT    NOT NULL,
    week                INTEGER NOT NULL,
    message_text        TEXT    NOT NULL,
    attempt             INTEGER NOT NULL DEFAULT 1,
    status              TEXT    NOT NULL CHECK (status IN ('sent', 'failed')),
    provider_message_id TEXT,
    delivery_status     TEXT,
    sent_at             TIMESTAMP,
    error_message       TEXT,
    created_at          TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS sms.opt_outs (
    id            SERIAL PRIMARY KEY,
    subjid        TEXT NOT NULL UNIQUE,
    mobile_number TEXT NOT NULL,
    reason        TEXT,
    opted_out_at  TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS sms_log_queue_id_idx ON sms.log (queue_id);
