from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class StageResult:
    success: bool
    rows_written: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[dict] = field(default_factory=list)


class BaseStage:
    name: str = ''
    dependencies: list[str] = []

    def __init__(self, config, engine):
        self.config = config
        self.engine = engine

    def run(self) -> StageResult:
        raise NotImplementedError(f"{self.__class__.__name__}.run() not implemented")
