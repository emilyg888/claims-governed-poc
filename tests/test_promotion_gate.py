"""Tests for promotion gate behavior."""

from __future__ import annotations

from datetime import date

from pipeline.controls.run_controls import ControlResult, promotion_gate


def test_gate_blocks_failed_block_control() -> None:
    """Gate should fail when a BLOCK control fails."""
    results = [
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C2",
            status="FAIL",
            blocking=True,
            severity="BLOCK",
            type="sql",
            fail_count=2,
        )
    ]
    assert promotion_gate(results) is False


def test_gate_allows_warn_failures() -> None:
    """Gate should pass when only WARN controls fail."""
    results = [
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C5",
            status="FAIL",
            blocking=False,
            severity="WARN",
            type="sql",
            fail_count=3,
        )
    ]
    assert promotion_gate(results) is True


def test_gate_blocks_when_any_block_fails_even_with_other_passes() -> None:
    """Any failed BLOCK control must stop promotion."""
    results = [
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C2_DQ_NON_NEGATIVE",
            status="FAIL",
            blocking=True,
            severity="BLOCK",
            type="sql",
            fail_count=1,
        ),
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C4_CLASSIFICATION_DOMAIN",
            status="PASS",
            blocking=True,
            severity="BLOCK",
            type="sql",
            fail_count=0,
        ),
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C5_EVENT_DOMAIN",
            status="FAIL",
            blocking=False,
            severity="WARN",
            type="sql",
            fail_count=2,
        ),
    ]
    assert promotion_gate(results) is False


def test_gate_passes_when_block_controls_pass() -> None:
    """Promotion can proceed when BLOCK controls pass, regardless of WARN."""
    results = [
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C2_DQ_NON_NEGATIVE",
            status="PASS",
            blocking=True,
            severity="BLOCK",
            type="sql",
            fail_count=0,
        ),
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C4_CLASSIFICATION_DOMAIN",
            status="PASS",
            blocking=True,
            severity="BLOCK",
            type="sql",
            fail_count=0,
        ),
        ControlResult(
            run_id="run_1",
            batch_date=date(2026, 2, 21),
            control_id="C5_EVENT_DOMAIN",
            status="FAIL",
            blocking=False,
            severity="WARN",
            type="sql",
            fail_count=1,
        ),
    ]
    assert promotion_gate(results) is True
