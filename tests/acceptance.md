AC1: If claim_amount_incurred < 0 → C2_DQ fails and record written to CTRL.EXCEPTIONS

AC2: If required columns missing → load fails before COPY

AC3: If all required controls pass → promotion to INT executes

AC4: CTRL.RUN_AUDIT must contain 1 record per run
