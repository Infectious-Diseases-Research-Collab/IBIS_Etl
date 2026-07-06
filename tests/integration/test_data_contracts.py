import pytest
from sqlalchemy import text

pytestmark = pytest.mark.integration


def _columns(engine, schema: str, table: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = :schema AND table_name = :table"
        ), {'schema': schema, 'table': table}).fetchall()
    return {r[0] for r in rows}


@pytest.mark.parametrize('schema,table', [
    ('gold_ibis', 'baseline'),
    ('ibis', 'baseline'),
    ('gold_ibis', 'followup'),
    ('ibis', 'followup'),
])
def test_baseline_and_followup_keep_run_uuid_but_drop_other_tracking_columns(pipeline_engine, schema, table):
    """Guards the gold-layer lineage design: run_uuid is a deliberate join
    key back to bronze_ibis.meta / silver_ibis.<table>_history, but the
    other ETL-internal tracking columns must not leak into tables PIs and
    analysts query directly."""
    cols = _columns(pipeline_engine, schema, table)
    assert 'run_uuid' in cols
    assert 'file_name' not in cols
    assert 'file_path' not in cols
    assert 'extracted_at' not in cols
    assert 'updated_at' not in cols


@pytest.mark.parametrize('schema', ['gold_ibis', 'ibis'])
def test_d_enrollment_has_exactly_its_documented_columns(pipeline_engine, schema):
    """d_enrollment is a narrow, explicit-column-list dimension table — its
    exact column set is a real contract with anything downstream that
    queries it, not an incidental side effect of a SELECT *."""
    cols = _columns(pipeline_engine, schema, 'd_enrollment')
    expected = {
        'uniqueid', 'subjid', 'screening_id', 'countrycode', 'tabletnum',
        'health_facility', 'consent', 'vdate', 'starttime', 'stoptime',
        'interviewer_id', 'run_uuid',
    }
    assert cols == expected
