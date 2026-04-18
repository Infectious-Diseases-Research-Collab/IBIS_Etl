#!/bin/bash
set -euo pipefail

# Write crontab entries from config.json so schedule changes only need a restart
PIPELINE_CRON=$(python3 -c "
import json, sys
try:
    c = json.load(open('/app/config.json'))
    val = c['schedule']['pipeline_cron']
    if not val or not val.strip():
        raise ValueError('pipeline_cron is empty')
    print(val.strip())
except Exception as e:
    print('ERROR: ' + str(e), file=sys.stderr)
    sys.exit(1)
")

STORE_CRON=$(python3 -c "
import json, sys
try:
    c = json.load(open('/app/config.json'))
    val = c['schedule']['store_cron']
    if not val or not val.strip():
        raise ValueError('store_cron is empty')
    print(val.strip())
except Exception as e:
    print('ERROR: ' + str(e), file=sys.stderr)
    sys.exit(1)
")

# Explicit emptiness guard: belt-and-suspenders against edge cases in set -e + $()
if [ -z "$PIPELINE_CRON" ]; then
    echo "FATAL: PIPELINE_CRON is empty — check config.json schedule.pipeline_cron" >&2
    exit 1
fi
if [ -z "$STORE_CRON" ]; then
    echo "FATAL: STORE_CRON is empty — check config.json schedule.store_cron" >&2
    exit 1
fi

cat > /etc/cron.d/ibis <<EOF
PATH=/usr/local/bin:/usr/bin:/bin
${PIPELINE_CRON} root cd /app && python ibis.py -a >> /var/log/ibis_pipeline.log 2>&1
${STORE_CRON} root cd /app && python ibis.py -p store_ibis >> /var/log/ibis_store.log 2>&1

EOF

chmod 0644 /etc/cron.d/ibis
mkdir -p /var/log/ibis
touch /var/log/ibis_pipeline.log /var/log/ibis_store.log

exec "$@"
