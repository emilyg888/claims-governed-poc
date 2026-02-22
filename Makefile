PYTHON ?= ~/envs/snowpark/bin/python

.PHONY: test lint run

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m compileall pipeline tests

run:
	$(PYTHON) -m pipeline.orchestrator.nightly_job --batch-date $${BATCH_DATE}
