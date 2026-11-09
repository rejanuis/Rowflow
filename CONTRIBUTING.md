# Contributing to Rowflow

Thanks for helping. Small fixes, new task types, docs and bug reports are all welcome.

## Before you start

- Open an issue for larger changes so we can agree on the approach first.
- Never commit real secrets (`.env`, keys, certificates). `.gitignore` already excludes them.

## Development setup

Follow [docs/GETTING_STARTED.md](docs/GETTING_STARTED.md) for a local stack. To run the tests without Docker:

```bash
python -m venv .venv && source .venv/bin/activate
pip install pytest                                    # validation tests only
pip install apache-airflow apache-airflow-providers-google fakeredis   # also runs the generator tests
pytest
```

Generator tests are skipped automatically when Airflow is not installed. SQL changes can be checked against a throwaway Postgres: load `sql/001_schema.sql`, `002_sample_data.sql`, `003_views.sql` in order.

## Adding a task type

Create one folder under `dags/dag_factory/task_types/` with `spec.py`, `build.py` and a `README.md`: see [task_types/README.md](dags/dag_factory/task_types/README.md). Include a sample row and document every setting.

## Pull request checklist

- [ ] New task types follow the one-folder layout (`spec.py` pure Python, `build.py` imports providers inside `build()`)
- [ ] `pytest` passes; `validation.py` and `task_types/*/spec.py` still import without Airflow installed
- [ ] SQL files still load in order on an empty database
- [ ] New settings or task types are documented
- [ ] No secrets, hostnames or personal data in the diff
- [ ] Describe how you tested it (and what you could not test)

A CI workflow is on the roadmap; contributions there are welcome.
