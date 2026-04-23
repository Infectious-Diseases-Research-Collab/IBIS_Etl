from __future__ import annotations

import logging

from modules.notifier import send_sms_flagged_alert
from modules.sms_processor import SmsProcessor
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class FetchDlr(BaseStage):
    name = 'fetch_dlr'
    dependencies: list[str] = ['send_sms']  # logical dependency; not part of --all

    def run(self) -> StageResult:
        processor = SmsProcessor(config=self.config, engine=self.engine)
        dlr = processor.fetch_delivery_statuses()

        logger.info(
            "DLR poll complete: checked=%d updated=%d pending=%d errors=%d",
            dlr.checked, dlr.updated, dlr.pending, len(dlr.errors),
        )

        flagged = processor.get_flagged_messages()
        if flagged:
            send_sms_flagged_alert(flagged, self.config, self.engine)

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
            },
        )
