# 🐂 + 🐍

Oxen Python Interface

## Build and Run

From the root of this repo, navigate to the oxen directory

```bash
cd oxen
```

Install requirements

```bash
pip install -r requirements.txt
```

Then use `maturin` to build the Rust native module and deploy it in the current virtualenv.

```bash
maturin develop
```

## Run Tests

```bash
pytest -s tests/
```

## Code Quality

### Formatting

```bash
black .
```

### Linting

```bash
ruff .
```

