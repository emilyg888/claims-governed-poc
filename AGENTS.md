# AGENTS.md
Project: air-lab-ai-data-copilot

This document defines execution rules for Codex agents.

---

## 1. Python Environment (MANDATORY)

All Python execution MUST use the project-local virtual environment:

    .venv/bin/python

Never use:
    ~/envs/*
    system python
    /usr/bin/python
    conda environments

Before running any Python code, ensure:

    source .venv/bin/activate

If executing directly, always call:

    .venv/bin/python <script>.py

---

## 2. Package Management

Install packages only inside `.venv`.

Use:

    .venv/bin/pip install <package>

Never install packages globally.

---

## 3. Project Structure Awareness

Key runtime areas:

    07_runtime_local/
        rag/
        query_engine/
        copilot/

Governance and semantic layers are read-only during runtime.

Never modify:
    01_governance/*
    02_semantic_layer/*

without explicit instruction.

---

## 4. Deterministic Execution Rule

All SQL execution must flow through:

    07_runtime_local/query_engine/execute_query.py

Do not execute ad-hoc SQL outside the governed query engine.

---

## 5. Environment Validation Check

Before running Python, validate interpreter:

    .venv/bin/python -c "import sys; print(sys.executable)"

Expected output must contain:

    /.venv/bin/python

If not, stop execution and correct environment.

---

## 6. Safety & Governance Guardrail

The LLM must never:
- Execute SQL directly
- Access raw tables
- Bypass policy enforcement

All data access must flow through governed views.

---

End of agent configuration.