# Data Fetcher

A Python package for fetching financial data from various sources including crypto exchanges, Alpaca, and Polygon.

## Publishing to PyPI

Follow these steps to publish a new version to PyPI:

1. Update version in `setup.py`
```python
setup(
    name='data_fetcher',
    version='0.1.8',  # Increment this
    ...
)
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install build tools:
```bash
pip install --upgrade pip
pip install build twine
```

4. Build the package:
```bash
python -m build
```

5. Test the build (optional):
```bash
python -m twine check dist/*
```

6. Upload to TestPyPI (optional):
```bash
python -m twine upload --repository testpypi dist/*
```

7. Upload to PyPI:
```bash
python -m twine upload dist/*
```

You'll need PyPI credentials to upload. You can store them in `~/.pypirc` or enter them when prompted.

## Development

1. Clone the repository
2. Create a virtual environment
3. Install development dependencies:
```bash
pip install -e ".[dev]"
```

## License

MIT
