from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from testcontainers.postgres import PostgresContainer

from modules.db import SCHEMAS, init_schemas, init_sms_tables, run_migrations
from stages.bronze_to_silver import BronzeToSilver
from stages.measures_ibis import MeasuresIbis
from stages.promote_ibis import PromoteIbis
from stages.store_ibis import StoreIbis
from stages.transform_ibis import TransformIbis


@pytest.fixture(scope='session')
def postgres_container():
    """One throwaway Postgres 16 container for the whole integration test
    session — container startup is the slow part, so this is started once,
    not per test."""
    with PostgresContainer('postgres:16') as container:
        yield container


@pytest.fixture
def clean_engine(postgres_container):
    """
    A SQLAlchemy engine pointed at the shared container, with every schema
    this pipeline owns dropped and rebuilt from scratch via the pipeline's
    own real bootstrap (init_schemas / run_migrations / init_sms_tables) —
    the same sequence ibis.py's main() runs on every real startup. Each
    test using this fixture starts from a known-clean, fully-migrated
    schema; drop-and-recreate is used instead of a rolled-back transaction
    because several stages (promote_ibis, store_ibis) run their own
    multi-statement engine.begin() transactions and DDL swaps internally,
    which don't compose with an outer test-level transaction.
    """
    engine = create_engine(postgres_container.get_connection_url())
    with engine.begin() as conn:
        for schema in SCHEMAS:
            conn.execute(text(f'DROP SCHEMA IF EXISTS {schema} CASCADE'))
    init_schemas(engine)
    run_migrations(engine)
    init_sms_tables(engine)
    yield engine
    engine.dispose()


_FIXTURE_CONFIG = {
    'trial': {
        'dedup_key': 'uniqueid',
        'country_code_map': {'uganda': 1},
    },
}


@pytest.fixture
def pipeline_engine(clean_engine):
    """
    Seeds one realistic participant directly into bronze_ibis (skipping
    FtpToExtracted/MdbToBronze/SendSms — see spec §2.2 for why: they have
    real external dependencies that don't fit a hermetic test and already
    have solid mocked-unit-test coverage), then runs the real
    BronzeToSilver -> TransformIbis -> MeasuresIbis -> PromoteIbis ->
    StoreIbis stage classes against the real container. Yields the engine
    afterward for assertions. Shared by the end-to-end test and the
    data-contract tests — the pipeline runs exactly once per test.
    """
    extracted_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    run_uuid = 'e2e-run-0001'

    baseline_row = pd.DataFrame([{
        'uniqueid': 'e2e-test-001',
        'countrycode': '1',
        'country': 'uganda',
        'community': 'Mbarara',
        'subjid': 'SUBJ001',
        'screening_id': 'SCR001',
        'tabletnum': '53',
        'health_facility': '11',
        'health_facility_ug': '11',
        'health_facility_ke': None,
        'consent': '1',
        'vdate': '01/01/2026',
        'starttime': '01/01/2026 09:00:00',
        'stoptime': '01/01/2026 09:30:00',
        'interviewer_id': 'INT01',
        'arm_text': 'HIV Risk Assessment',
        'mobile_number': '256700000001',
        'preferred_language_text': 'English',
        'next_appt_3m': '01/04/2026 00:00:00',
        'next_appt_6m': '01/07/2026 00:00:00',
        'run_uuid': run_uuid,
        'file_name': 'e2e_fixture.mdb',
        'file_path': '/fake/e2e_fixture.mdb',
        'extracted_at': extracted_at,
    }])
    meta_row = pd.DataFrame([{
        'run_uuid': run_uuid,
        'file_name': 'e2e_fixture.mdb',
        'file_path': '/fake/e2e_fixture.mdb',
        'table_name': 'baseline',
        'country': 'uganda',
        'community': 'Mbarara',
        'extracted_at': extracted_at,
        'last_modified': extracted_at,
        'loaded': True,
        'promoted_to_silver_at': None,
    }])

    # sql/transform/followup.sql unconditionally reads FROM silver_ibis.followup,
    # so that table must exist by the time TransformIbis runs — same as in real
    # runs, where every MDB extraction always produces both a baseline and a
    # followup batch. This fixture only has a real baseline participant, so an
    # empty (0-row, baseline-shaped) followup batch is seeded too, matching the
    # same "empty df, same columns as baseline" shape used by
    # tests/test_bronze_to_silver.py's own mocked-followup fixtures. This makes
    # BronzeToSilver create silver_ibis.followup/_history as empty tables via
    # its normal zero-row to_sql-creates-the-table path, exactly as it would on
    # a real, otherwise-empty followup extraction.
    followup_run_uuid = 'e2e-run-0002'
    followup_row = baseline_row.iloc[0:0].copy()
    followup_row['run_uuid'] = followup_run_uuid
    followup_meta_row = pd.DataFrame([{
        'run_uuid': followup_run_uuid,
        'file_name': 'e2e_fixture.mdb',
        'file_path': '/fake/e2e_fixture.mdb',
        'table_name': 'followup',
        'country': 'uganda',
        'community': 'Mbarara',
        'extracted_at': extracted_at,
        'last_modified': extracted_at,
        'loaded': True,
        'promoted_to_silver_at': None,
    }])

    with clean_engine.begin() as conn:
        baseline_row.to_sql('baseline', conn, schema='bronze_ibis', if_exists='append', index=False)
        followup_row.to_sql('followup', conn, schema='bronze_ibis', if_exists='append', index=False)
        meta_row.to_sql('meta', conn, schema='bronze_ibis', if_exists='append', index=False)
        followup_meta_row.to_sql('meta', conn, schema='bronze_ibis', if_exists='append', index=False)

    for stage_cls in (BronzeToSilver, TransformIbis, MeasuresIbis, PromoteIbis, StoreIbis):
        result = stage_cls(config=_FIXTURE_CONFIG, engine=clean_engine).run()
        assert result.success, f"{stage_cls.__name__} failed: {result.errors}"

    yield clean_engine
