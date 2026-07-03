"""
Confirms Uganda/Kenya facility reference data has one source of truth
(modules.reference_data) rather than being hand-copied into
data_validator.py, measures_ibis.py, and the two Uganda export scripts —
the exact duplication a prior review flagged as a maintenance hazard
(a new facility opening would require editing all four independently).
"""
from __future__ import annotations

from modules import reference_data


def test_facility_dicts_have_expected_shape():
    assert reference_data.FACILITY_CODES_KE[99] == 'Other'
    assert reference_data.FACILITY_CODES_UG[11] == 'Bushenyi HCIV'


def test_data_validator_uses_shared_reference_data_by_identity():
    """Not just equal values — the *same object* on both sides, so a future
    edit to reference_data.py can't silently diverge from data_validator.py."""
    from modules.data_validator import DataValidator

    assert DataValidator._FACILITY_CODES_KE is reference_data.FACILITY_CODES_KE
    assert DataValidator._FACILITY_CODES_UG is reference_data.FACILITY_CODES_UG


def test_measures_ibis_facility_config_uses_shared_reference_data():
    from stages.measures_ibis import _FACILITY_CONFIG

    _, ke_map = _FACILITY_CONFIG['kenya']
    _, ug_map = _FACILITY_CONFIG['uganda']
    assert ke_map is reference_data.FACILITY_CODES_KE
    assert ug_map is reference_data.FACILITY_CODES_UG


def test_incentive_export_script_uses_shared_reference_data():
    import scripts.export_ug_incentive_arm as mod
    assert mod.UG_FACILITY_LABELS is reference_data.FACILITY_CODES_UG


def test_occupation_export_script_uses_shared_reference_data():
    import scripts.export_ug_occupation_97 as mod
    assert mod.UG_FACILITY_LABELS is reference_data.FACILITY_CODES_UG
