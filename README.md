# aegisflow-quant

Async-first crypto quant trading infrastructure bootstrap.

## Overview

This repository is a production-ready Python 3.12 starter for a crypto quant trading system.
It is designed to be minimal, modular, and ready for:

- websocket collectors
- parquet storage
- multi-exchange data pipelines
- structured logging
- graceful shutdown handling

## Features

- `uvloop` for fast async event loops
- `structlog` structured JSON logging
- `pydantic` settings and environment loading
- async application bootstrap and shutdown flow
- abstract base collector ready for exchange-specific implementations

## Project structure

```text
aegisflow-quant/
│
├── collectors/
│   ├── __init__.py
│   └── base.py
│
├── data/
│   ├── raw/
│   └── parquet/
│
├── utils/
│   ├── __init__.py
│   ├── logger.py
│   ├── signals.py
│   └── time.py
│
├── configs/
│   ├── settings.py
│   └── .env.example
│
├── tests/
├── requirements.txt
├── main.py
└── README.md
```

## Getting started

1. Create a virtual environment for Python 3.12.
2. Install dependencies:

```bash
python -m pip install -r requirements.txt
```

3. Copy `configs/.env.example` to `.env` and customize values as needed.
4. Run the application:

```bash
python main.py
```

Press `Ctrl+C` to trigger a graceful shutdown.

## Testing

```bash
pytest
```
