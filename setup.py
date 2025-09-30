"""
MongoDB Wizard - Setup configuration
"""

from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and not line.startswith("#")]

setup(
    name="mongo-wizard",
    version="1.0.0",
    author="Sathia Musso",
    description="Advanced MongoDB copy and migration tool",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/sathia-musso/mongo-wizard",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "mongo-wizard=mongo_wizard.cli:main",
            "mw=mongo_wizard.cli:main",  # Short alias
        ],
    },
)