#!/bin/bash
set -e

# Write crontab entries from config.json so schedule changes only need a restart
PIPELINE_CRON=$(python3 -c "
import json, sys
try:
    c = json.load(open('/app/config.json'))
    print(c['schedule']['pipeline_cron'])
except (KeyError, FileNotFoundError) as e:
    print('ERROR: config.json missing schedule.pipeline_cron: ' + str(e), file=sys.stderr)
    sys.exit(1)
")
STORE_CRON=$(python3 -c "
import json, sys
try:
    c = json.load(open('/app/config.json'))
    print(c['schedule']['store_cron'])
except (KeyError, FileNotFoundError) as e:
    print('ERROR: config.json missing schedule.store_cron: ' + str(e), file=sys.stderr)
    sys.exit(1)
")

cat > /etc/cron.d/ibis <<EOF
PATH=/usr/local/bin:/usr/bin:/bin
IBIS_DB_PASSWORD=${IBIS_DB_PASSWORD}
${PIPELINE_CRON} root cd /app && python ibis.py -a >> /var/log/ibis_pipeline.log 2>&1
${STORE_CRON} root cd /app && python ibis.py -p store_ibis >> /var/log/ibis_store.log 2>&1

EOF

chmod 0644 /etc/cron.d/ibis
touch /var/log/ibis_pipeline.log /var/log/ibis_store.log

exec "$@"
