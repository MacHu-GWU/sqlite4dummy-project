#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Quick Link:

- GitHub: https://github.com/MacHu-GWU/sqlite4dummy-project
- PyPI: https://pypi.python.org/pypi/sqlite4dummy
- Document: http://sqlite4dummy-project.readthedocs.org/
"""

from .dtype import dtype
from .engine import Sqlite3Engine
from .row import Row
from .schema import (Column, Table, Index, MetaData,
    Insert, Select, Update, Delete)
from .func import func
from .sql import and_, or_, asc, desc

__version__ = "0.0.5"
__all__ = [
    "dtype", 
    "Sqlite3Engine", 
    "Row",
    "Column", "Table", "Index", "MetaData", "Insert", "Select", "Update", "Delete",
    "func",
    "and_", "or_", "asc", "desc",
]