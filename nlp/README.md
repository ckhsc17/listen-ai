# NLP Service (FastAPI)

This module performs simple lexicon-based sentiment analysis for English and Traditional Chinese text.

## Prerequisites

- Python 3.11+

## Run Without Docker

1. Open a terminal in this folder:

```bash
cd nlp
```

2. Create and activate a virtual environment:

```bash
python -m venv .venv
source .venv/bin/activate
```

3. Install dependencies:

```bash
pip install -r requirements.txt
```

4. (Optional) Configure port:

```bash
export NLP_PORT=8001
```

5. Start the API:

```bash
uvicorn app:app --host 0.0.0.0 --port ${NLP_PORT:-8001}
```

## Health Check

```bash
curl http://localhost:8001/health
```

## Example Request

```bash
curl -X POST http://localhost:8001/sentiment \
  -H "Content-Type: application/json" \
  -d '{"texts":["great update","bad experience","這次更新很好","體驗很糟"]}'
```

## Run Unit Tests

```bash
python -m unittest -v
```
