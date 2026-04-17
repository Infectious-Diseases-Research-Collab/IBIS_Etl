from __future__ import annotations

import logging

from modules.sms_processor import SmsProcessor
from stages.base import BaseStage, StageResult

logger = logging.getLogger(__name__)


class SendSms(BaseStage):
    name = 'send_sms'
    dependencies: list[str] = ['promote_ibis']

    def run(self) -> StageResult:
        processor = SmsProcessor(config=self.config, engine=self.engine)
        sms = processor.run()

        errors = [
            f"subjid={f['subjid']} mobile={f['mobile_number']} week={f['week']}: {f['error']}"
            for f in sms.failures
        ]

        return StageResult(
            success=sms.failed == 0,
            rows_written=sms.sent,
            errors=errors,
            metadata={
                'sent': sms.sent,
                'failed': sms.failed,
                'skipped': sms.skipped,
                'failures': sms.failures,
            },
        )
