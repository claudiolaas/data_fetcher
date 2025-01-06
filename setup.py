from setuptools import setup, find_packages

setup(
    name='data_fetcher',
    version='0.1.9',
    description='A Python package for fetching financial data from various sources',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Your Name',
    author_email='your.email@example.com',
    url='https://github.com/yourusername/data_fetcher',
    packages=find_packages(),
    install_requires=[
        'ccxt',
        'pandas',
        'numpy',
        'alpaca-trade-api',
        'alpaca-py',
        'python-dateutil',
        'tenacity',
    ],
    extras_require={
        'dev': [
            'pytest',
            'pytest-cov',
            'black',
            'isort',
            'flake8',
            'build',
            'twine',
        ],
    },
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
    entry_points={
        'console_scripts': [
            'data_fetcher=data_fetcher.cli:main',
        ],
    },
)
