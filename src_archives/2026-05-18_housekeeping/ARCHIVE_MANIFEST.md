# Archive Manifest - 2026-05-18 Housekeeping

## Archived items

| Original path | Archived path | Reason | Validation performed | Risk |
|---|---|---|---|---|
| `build/` | `src_archives/2026-05-18_housekeeping/build/` | Generated setuptools build output. | No repo references found with `rg`; files mirror active source modules under `config/`, `pipeline/`, and `scripts/`; SIT passed before archive. | Low |
| `notes_ai/` | `src_archives/2026-05-18_housekeeping/notes_ai/` | Untracked duplicate notes/raw document folder. | No repo references found with `rg`; raw Markdown and PNG files were checksum-identical to files under `docs/`; SIT passed before archive. | Low |
| `docs/ModernDataPipeline.drawio v1.png` | `src_archives/2026-05-18_housekeeping/docs/ModernDataPipeline.drawio v1.png` | Untracked duplicate/generated image artifact. | No repo references found with `rg`; checksum-identical copy existed under `notes_ai/raw/`; SIT passed before archive. | Low |

## Notes

- Items were moved, not deleted.
- The working tree already contained unrelated Streamlit edits before housekeeping started.
- `claims_governed_poc.egg-info/` exists locally as ignored generated package metadata and is tracked as a pending cleanup item rather than archived.
