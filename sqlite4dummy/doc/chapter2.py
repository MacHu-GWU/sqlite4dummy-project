#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlite4dummy import *
import sqlite3
import os

def reset():
    try:
        os.remove("test.db")
    except:
        pass
    
reset()

# metadata = MetaData()
# number = Table("number", metadata,
#     Column("_id", dtype.INTEGER, primary_key=True),
#     Column("value", dtype.REAL),
#     )
# article = Table("article", metadata,
#     Column("_id", dtype.INTEGER, primary_key=True),
#     Column("content", dtype.TEXT),
#     )
# engine = Sqlite3Engine("test.db") # use autocommit default True
# metadata.create_all(engine)

# print(engine.all_tablename) # before
# article.drop(engine)
# print(engine.all_tablename) # after
# 
# print(engine.all_tablename) # before: ['number']
# metadata.drop_all(engine)
# print(engine.all_tablename) # after: []

# number_value_index = Index("number_value_index", 
#                            metadata, number.c.value.desc())
# number_value_index.create(engine)
# 
# article_content_index = Index("article_content_index", 
#                               metadata, article.c.content) # default ascending
# article_content_index.create(engine)

# print(engine.all_indexname)


reset()

connect = sqlite3.connect("test.db")
cursor = connect.cursor()
cursor.execute("CREATE TABLE number (_id INTEGER PRIMARY KEY, value REAL)")
cursor.execute("CREATE TABLE article (_id INTEGER PRIMARY KEY, content TEXT)")
cursor.execute("CREATE INDEX number_value_index ON number (value)")
cursor.execute("CREATE INDEX article_content_index ON article (content DESC)")
connect.commit()

metadata = MetaData()
engine = Sqlite3Engine("test.db")
metadata.reflect(engine)
print(metadata)