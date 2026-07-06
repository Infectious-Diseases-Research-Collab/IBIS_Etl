import json
import logging

from modules.logging_utils import JsonFormatter


def _make_record(msg='hello', extra=None, level=logging.INFO):
    record = logging.LogRecord(
        name='test.logger', level=level, pathname=__file__, lineno=1,
        msg=msg, args=(), exc_info=None,
    )
    if extra:
        for k, v in extra.items():
            setattr(record, k, v)
    return record


def test_json_formatter_produces_valid_json_with_standard_fields():
    formatter = JsonFormatter()
    record = _make_record('hello world')
    output = json.loads(formatter.format(record))

    assert output['message'] == 'hello world'
    assert output['level'] == 'INFO'
    assert output['logger'] == 'test.logger'
    assert 'timestamp' in output


def test_json_formatter_merges_extra_fields():
    formatter = JsonFormatter()
    record = _make_record('stage finished', extra={'stage': 'transform_ibis', 'rows_written': 42})
    output = json.loads(formatter.format(record))

    assert output['stage'] == 'transform_ibis'
    assert output['rows_written'] == 42
    assert output['message'] == 'stage finished'


def test_json_formatter_does_not_leak_internal_logrecord_attributes():
    """Only standard fields + genuinely user-supplied extras should appear —
    not every internal LogRecord attribute (pathname, funcName, etc.)."""
    formatter = JsonFormatter()
    record = _make_record('plain message')
    output = json.loads(formatter.format(record))

    assert 'pathname' not in output
    assert 'funcName' not in output
    assert 'args' not in output
