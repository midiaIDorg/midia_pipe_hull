#!/usr/bin/env python3
from glob import glob

from setuptools import find_packages, setup

setup(
    name="midia_pipe_hull",
    version="0.0.1",
    url="https://github.com/midiaIDorg/midia_pipe_hull",
    author="Mateusz Krzysztof Łącki, Michał Piotr Startek",
    author_email="matteo.lacki@gmail.com, michal.startek@mimuw.edu.pl",
    description="A silly will hull for dependencies of midiaID.",
    packages=find_packages(),
    install_requires=[
        "csvkit",
        "ipykernel",
        "ipython",
        "jupyter",
        "jupyterlab",
        "matplotlib",
        "openpyxl",
        "opentims_bruker_bridge",
        "pandas",
        "plotly",
        "plotnine",
        "pony",
        "pymysql",
        "pytest",
        "seaborn",
        "snakemake",
        "virtualenvwrapper",
        "pyqt5",
        "ipympl",
        "cowsay",
        "UpSetPlot",
        "duckdb",
    ],
    scripts=glob("tools/*.py"),
)
