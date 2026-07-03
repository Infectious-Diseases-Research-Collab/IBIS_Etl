# modules/reference_data.py
"""
Single source of truth for trial reference data that would otherwise be
hand-copied across modules — health facility code -> name mappings, in
particular. Before this module existed, the Uganda facility list was
independently defined in modules/data_validator.py, scripts/
export_ug_incentive_arm.py, and scripts/export_ug_occupation_97.py; a new
facility opening meant remembering to edit all three (or, more likely,
someone updating one and the others silently drifting out of date).

Import from here; do not redefine these dicts locally.
"""
from __future__ import annotations

FACILITY_CODES_KE: dict[int, str] = {
    21: 'Homa Bay Teaching and Referral Hospital',
    22: 'Rachuonyo District Hospital',
    23: 'Suba District Hospital',
    24: 'Ndhiwa District Hospital',
    99: 'Other',
}

FACILITY_CODES_UG: dict[int, str] = {
    11: 'Bushenyi HCIV',
    12: 'Ishaka Adventist Hospital (Bushenyi)',
    13: 'Ishongororo HCIV (Ibanda)',
    14: 'Ruhoko HCIV (Ibanda)',
    99: 'Other',
}
