"""Tests for promotion gate behavior."""

from __future__ import annotations

from pipeline.controls.run_controls import ControlResult, promotion_gate


def test_gate_blocks_failed_block_control() -> None:
    """Gate should fail when a BLOCK control fails."""
    results = [ControlResult("C2", "BLOCK", 2, "FAIL")]
    assert promotion_gate(results) is False


def test_gate_allows_warn_failures() -> None:
    """Gate should pass when only WARN controls fail."""
    results = [ControlResult("C5", "WARN", 3, "FAIL")]
    assert promotion_gate(results) is True


def test_gate_blocks_when_any_block_fails_even_with_other_passes() -> None:
    """Any failed BLOCK control must stop promotion."""
    results = [
        ControlResult("C2_DQ_NON_NEGATIVE", "BLOCK", 1, "FAIL"),
        ControlResult("C4_CLASSIFICATION_DOMAIN", "BLOCK", 0, "PASS"),
        ControlResult("C5_EVENT_DOMAIN", "WARN", 2, "FAIL"),
    ]
    assert promotion_gate(results) is False


def test_gate_passes_when_block_controls_pass() -> None:
    """Promotion can proceed when BLOCK controls pass, regardless of WARN."""
    results = [
        ControlResult("C2_DQ_NON_NEGATIVE", "BLOCK", 0, "PASS"),
        ControlResult("C4_CLASSIFICATION_DOMAIN", "BLOCK", 0, "PASS"),
        ControlResult("C5_EVENT_DOMAIN", "WARN", 1, "FAIL"),
    ]
    assert promotion_gate(results) is True
