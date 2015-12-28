#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本例用于演示在一对多的关系中, Left Join的常见用法
"""

import sqlite3
import unittest
from prettytable import from_db_cursor

connect = sqlite3.connect(":memory:")
cursor = connect.cursor()
cursor.execute("CREATE TABLE user (user_id INTEGER, name TEXT)")
cursor.execute("CREATE TABLE post (post_id INTEGER PRIMARY KEY, title TEXT, user_id INTEGER, FOREIGN KEY(user_id) REFERENCES user(user_id))")

data = [(1, "David"), (2, "John"), (3, "Mike")]
cursor.executemany("INSERT INTO user VALUES (?,?)", data)

data = [(1, "Where to buy some fruits?", 1), 
        (2, "Friday party recruiting!", 1), 
        (3, "Looking for roommate.", 2),
        (4, "Vote me student president!", 3),]
cursor.executemany("INSERT INTO post VALUES (?,?,?)", data)

# 打印出user_id = 1的用户的所有帖子的标题
query = \
"""
SELECT post.title FROM post WHERE post.user_id == 1
"""
cursor.execute(query)
print(from_db_cursor(cursor))

# 打印出post_id = 1的帖子的发帖用户的用户名
query = \
"""
SELECT user.name 
FROM user
LEFT JOIN post
ON user.user_id=post.user_id
WHERE post.post_id=1  
"""
cursor.execute(query)
print(from_db_cursor(cursor))