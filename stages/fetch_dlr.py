from __future__ import annotations

import logging

from modules.notifier import send_sms_flagged_alert
from modules.sms_processor import SmsProcessor
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class FetchDlr(BaseStage):
    """
    Polls Blasta for delivery status of previously sent messages and alerts
    the data manager about anything that never reached the provider.

    Invoked from sms.py's --check-delivery flag on its own cron schedule
    (dlr_cron) — deliberately not part of ibis.py's stage graph or --all run.
    DLR status often isn't available until well after a message is sent, so
    checking it needs to run on a separate, later schedule rather than in
    the same nightly pipeline pass that calls SendSms.
    """
    name = 'fetch_dlr'

    def run(self) -> StageResult:
        processor = SmsProcessor(config=self.config, engine=self.engine)
        dlr = processor.fetch_delivery_statuses()

        logger.info(
            "DLR poll complete: checked=%d updated=%d pending=%d errors=%d",
            dlr.checked, dlr.updated, dlr.pending, len(dlr.errors),
        )

        flagged = processor.get_flagged_messages()
        flagged_alert_sent = None
        if flagged:
            flagged_alert_sent = send_sms_flagged_alert(flagged, self.config, self.engine)

        errors = [
            f"log_id={e['log_id']} subjid={e.get('subjid','')} msg_id={e.get('provider_message_id','')}: {e['error']}"
            for e in dlr.errors
        ]

        # Fail only when every checked row errored
        all_failed = (
            dlr.checked > 0
            and dlr.updated == 0
            and dlr.pending == 0
            and len(dlr.errors) == dlr.checked
        )

        return StageResult(
            success=not all_failed,
            rows_written=dlr.updated,
            errors=errors,
            metadata={
                'checked': dlr.checked,
                'updated': dlr.updated,
                'pending': dlr.pending,
                'errors': dlr.errors,
                'flagged': len(flagged),
                'flagged_alert_sent': flagged_alert_sent,
            },
        )
