#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本例用于演示在多对多的关系中, Left Join的常见用法
"""

import sqlite3
import unittest
from prettytable import from_db_cursor

connect = sqlite3.connect(":memory:")
cursor = connect.cursor()
cursor.execute("CREATE TABLE movie (movie_id INTEGER PRIMARY KEY, title TEXT)")
cursor.execute("CREATE TABLE gener (gener_id INTEGER PRIMARY KEY, name TEXT)")
cursor.execute("""
    CREATE TABLE movie_gener(
        movie_id INTEGER, 
        gener_id INTEGER,
        FOREIGN KEY(movie_id) REFERENCES movie(movie_id),
        FOREIGN KEY(gener_id) REFERENCES gener(gener_id),
        PRIMARY KEY (movie_id, gener_id)
    )
""")

data = [(1, "The Shawshank Redemption"),
        (2, "12 Angry Men"),
        (3, "Schindler's List"),]
cursor.executemany("INSERT INTO movie VALUES (?,?)", data)

data = [(1, "crime"),
        (2, "drama"),
        (3, "biography"),
        (4, "history"),]
cursor.executemany("INSERT INTO gener VALUES (?,?)", data)

data = [(1, 1), (1, 2),
        (2, 1), (2, 2),
        (3, 2), (3, 3), (3, 4),]
cursor.executemany("INSERT INTO movie_gener VALUES (?,?)", data)

# 打印出movie_id=1的所有gener name
query = \
"""
SELECT gener.name
FROM gener
LEFT JOIN movie_gener
ON gener.gener_id=movie_gener.gener_id
WHERE movie_gener.movie_id=1
"""
cursor.execute(query)
print(from_db_cursor(cursor))

# 打印出gener_id=1的所有movie title
query = \
"""
SELECT movie.title
FROM movie
LEFT JOIN movie_gener
ON movie.movie_id=movie_gener.movie_id
WHERE movie_gener.gener_id=1
"""
cursor.execute(query)
print(from_db_cursor(cursor))
