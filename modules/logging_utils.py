from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

# The standard attributes every logging.LogRecord carries — anything else set
# on a record (via logger.info(msg, extra={...})) is a genuine caller-supplied
# extra field and gets merged into the JSON output.
_STANDARD_RECORD_ATTRS = frozenset(logging.LogRecord(
    name='', level=0, pathname='', lineno=0, msg='', args=(), exc_info=None,
).__dict__.keys()) | {'message', 'asctime'}


class JsonFormatter(logging.Formatter):
    """Formats each log record as one JSON object per line, merging any
    extra={...} fields a call site attached."""

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            'timestamp': datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
        }
        if record.exc_info:
            payload['exc_info'] = self.formatException(record.exc_info)

        for key, value in record.__dict__.items():
            if key not in _STANDARD_RECORD_ATTRS:
                payload[key] = value

        return json.dumps(payload, default=str)


def configure_logging(level: int = logging.INFO) -> None:
    """Configure the root logger with JsonFormatter. Call once per process,
    replacing logging.basicConfig(format=...) in each entry point."""
    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]
