"""Control register loader (rules/controls.yaml -> typed definitions)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from pipeline.controls.models import ControlDefinition


class ControlRegistry:
    """Loads and validates control definitions from YAML."""

    def __init__(self, path: str = "rules/controls.yaml") -> None:
        self.path = path

    def load_raw(self, register_path: str | None = None) -> dict[str, Any]:
        with Path(register_path or self.path).open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle) or {}
        if not isinstance(payload, dict):
            raise ValueError("rules/controls.yaml must contain a mapping")
        return payload

    def load(self, register_path: str | None = None) -> list[ControlDefinition]:
        payload = self.load_raw(register_path)
        controls = payload.get("controls", [])
        if not isinstance(controls, list):
            raise ValueError("rules/controls.yaml controls must be a list")

        definitions: list[ControlDefinition] = []
        for item in controls:
            if not isinstance(item, dict):
                continue
            control_id = str(item.get("control_id") or item.get("id") or "").strip()
            if not control_id:
                continue
            control_type = str(item.get("type", "sql")).strip().lower()
            if control_type not in {"precheck", "sql", "gate"}:
                raise ValueError(f"Unsupported control type for {control_id}: {control_type}")
            severity = str(item.get("severity", "BLOCK")).strip().upper()
            definitions.append(
                ControlDefinition(
                    control_id=control_id,
                    type=control_type,  # type: ignore[arg-type]
                    enabled=bool(item.get("enabled", True)),
                    blocking=bool(item.get("blocking", severity == "BLOCK")),
                    severity=severity,
                    description=str(item.get("description", control_id)),
                    sql_path=item.get("sql_path"),
                    params=item.get("params") if isinstance(item.get("params"), dict) else {},
                    threshold=float(item.get("threshold", 0)),
                    query=item.get("query"),
                )
            )
        return definitions
