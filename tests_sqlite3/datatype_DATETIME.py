#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from datetime import datetime
import sqlite3

connect = sqlite3.connect(":memory:", detect_types=sqlite3.PARSE_DECLTYPES)
cursor = connect.cursor()
cursor.execute("CREATE TABLE test (_id INTEGER PRIMARY KEY, create_time TIMESTAMP)")

now = datetime.now()
cursor.execute("INSERT INTO test VALUES (?,?)", (1, now))

print(cursor.execute("SELECT * FROM test").fetchone())
print(cursor.execute("SELECT * FROM test WHERE create_time = '%s';" % now).fetchone())
