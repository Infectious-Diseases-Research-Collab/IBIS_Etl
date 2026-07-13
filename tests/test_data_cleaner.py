import pandas as pd

from modules.data_cleaner import DataCleaner


def test_apply_field_override_sets_target_column_where_condition_matches():
    df = pd.DataFrame({
        'consent': ['-9', '1', '1'],
        'subjid': ['IBIS-1', 'IBIS-2', 'IBIS-3'],
    })
    result = DataCleaner(df).apply_field_override(
        when_col='consent', when_value=-9, set_col='subjid', set_value=-9,
    )

    assert list(result['subjid']) == ['-9', 'IBIS-2', 'IBIS-3']


def test_apply_field_override_does_not_mutate_original_dataframe():
    df = pd.DataFrame({
        'consent': ['-9'],
        'subjid': ['IBIS-1'],
    })
    DataCleaner(df).apply_field_override(
        when_col='consent', when_value=-9, set_col='subjid', set_value=-9,
    )

    assert df['subjid'].iloc[0] == 'IBIS-1'


def test_apply_field_override_is_noop_when_when_col_missing():
    df = pd.DataFrame({'subjid': ['IBIS-1']})
    result = DataCleaner(df).apply_field_override(
        when_col='consent', when_value=-9, set_col='subjid', set_value=-9,
    )

    assert list(result['subjid']) == ['IBIS-1']


def test_apply_field_override_is_noop_when_set_col_missing():
    df = pd.DataFrame({'consent': ['-9']})
    result = DataCleaner(df).apply_field_override(
        when_col='consent', when_value=-9, set_col='subjid', set_value=-9,
    )

    assert 'subjid' not in result.columns
