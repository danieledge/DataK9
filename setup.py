from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="data-validation-tool",
    version="0.1.0",
    author="Your Name",
    description="A robust data validation tool for pre-load data quality checks",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.9",
    install_requires=[
        "pandas>=2.0.0",
        "pyarrow>=12.0.0",
        "openpyxl>=3.1.0",
        "pyyaml>=6.0",
        "jinja2>=3.1.0",
        "click>=8.1.0",
        "jsonschema>=4.17.0",
        "tqdm>=4.65.0",
        "python-dateutil>=2.8.0",
        "colorama>=0.4.6",
    ],
    extras_require={
        "large-datasets": ["dask[dataframe]>=2023.5.0"],
    },
    entry_points={
        "console_scripts": [
            "data-validate=validation_framework.cli:cli",
        ],
    },
    package_data={
        "validation_framework": ["reporters/templates/*.html"],
    },
)
