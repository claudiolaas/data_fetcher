from setuptools import setup, find_packages

setup(
    name='data_fetcher',
    version='0.1.2',
    packages=find_packages(),
    install_requires=[
        'ccxt',
        'pandas',
        'numpy',
        'alpaca-trade-api',
        'alpaca-py',
        'python-dateutil',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            'your_package_name=your_package_name:main',
        ],
    },
)