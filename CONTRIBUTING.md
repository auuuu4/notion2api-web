# Contributing

## Development setup

```bash
python -m venv .venv
. .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env            # fill in local credentials
python -m unittest discover -s tests -v
```

Run the service locally with `uvicorn main:app --host 127.0.0.1 --port 8088`.
Do not expose the dashboard or management API to untrusted networks (see
[SECURITY.md](SECURITY.md)).

## Pull requests

- Keep changes focused and include tests for behavior changes.
- Run `python -m compileall -q app main.py scripts` and the full unittest suite.
- Do not commit secrets, account tokens, generated databases or captured
  transcripts. Use synthetic fixtures under `tests/fixtures/`.
- Preserve the existing protocol fixture format and update
  `scripts/capture_to_fixture.py` documentation when adding cases.
