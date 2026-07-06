-- sms.message_status: one row per participant/week, consolidated delivery
-- status and latest send details. Split out from init_sms_schema.sql because
-- this view joins ibis.baseline, which doesn't exist until promote_ibis has
-- run at least once — db.py's init_sms_tables() creates it separately and
-- conditionally, only once ibis.baseline exists (see init_sms_tables()).
CREATE OR REPLACE VIEW sms.message_status AS
SELECT
    q.subjid,
    q.mobile_number,
    q.week,
    q.arm_text,
    q.language,
    b.health_facility_ug,
    q.status                                                        AS queue_status,
    q.scheduled_date,
    latest.sent_at,
    latest.provider_message_id,
    COALESCE(
        MAX(l.delivery_status) FILTER (WHERE l.delivery_status = 'DELIVERED'),
        MAX(l.delivery_status) FILTER (WHERE l.delivery_status = 'FAILED'),
        MAX(l.delivery_status) FILTER (WHERE l.delivery_status = 'NOT_FOUND'),
        NULL
    )                                                               AS delivery_status,
    COUNT(l.id)                                                     AS attempts
FROM sms.queue q
JOIN ibis.baseline b ON b.subjid = q.subjid
LEFT JOIN sms.log l ON l.queue_id = q.id
LEFT JOIN LATERAL (
    SELECT sent_at, provider_message_id
    FROM sms.log
    WHERE queue_id = q.id AND status = 'sent'
    ORDER BY sent_at DESC
    LIMIT 1
) latest ON true
GROUP BY q.id, q.subjid, q.mobile_number, q.week, q.arm_text, q.language,
         b.health_facility_ug, q.status, q.scheduled_date,
         latest.sent_at, latest.provider_message_id;
