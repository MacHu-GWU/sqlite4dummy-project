#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup

SHORT_DESCRIPTION = ""

with open("README.rst", "rb") as f:
    LONG_DESCRIPTION = f.read().decode("utf-8")

VERSION = __import__("sqlite4dummy").__version__


setup(
    name = "sqlite4dummy",
    packages = [
        "sqlite4dummy",
        "sqlite4dummy.tests",
        ],
    package_data={"": ["LICENSE.txt"]},
    version = VERSION,
    description = "a high performance and easy to use Sqlite API for Data Scientist",
    long_description=LONG_DESCRIPTION,
    author = "Sanhe Hu",
    author_email = "husanhe@gmail.com",
    url = "https://github.com/MacHu-GWU/sqlite4dummy-project",
    download_url = "https://github.com/MacHu-GWU/sqlite4dummy-project/tarball/0.1",
    keywords = ["sqlite", "database", "data science"],
    license = "LGPL 3.0",
    classifiers = [],
)