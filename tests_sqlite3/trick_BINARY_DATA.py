#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sqlite3
import pickle

connect = sqlite3.connect(":memory:")
cursor = connect.cursor()
cursor.execute("CREATE TABLE (content BOLB)")