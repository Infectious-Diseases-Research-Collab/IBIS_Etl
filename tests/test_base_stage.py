from stages.base import BaseStage, StageResult

def test_stage_result_defaults():
    r = StageResult(success=True)
    assert r.rows_written == 0
    assert r.errors == []

def test_stage_result_failure():
    r = StageResult(success=False, errors=["something broke"])
    assert not r.success
    assert len(r.errors) == 1

def test_base_stage_run_raises():
    import pytest
    from unittest.mock import MagicMock
    stage = BaseStage(config=MagicMock(), engine=MagicMock())
    with pytest.raises(NotImplementedError):
        stage.run()

def test_base_stage_has_required_attributes():
    assert hasattr(BaseStage, 'name')
    assert hasattr(BaseStage, 'dependencies')
