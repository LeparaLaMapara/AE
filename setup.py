from setuptools import setup, find_packages

setup(
    name="analytics_engine",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark",
        "pyyaml",
        "joblib",
        "requests",
    ],
    entry_points={
        'console_scripts': [
            'analytics_engine=main:main',
        ],
    },
    author="Your Name",
    author_email="your.email@example.com",
    description="A Python package for running ETL pipelines and managing machine learning models using PySpark.",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)
