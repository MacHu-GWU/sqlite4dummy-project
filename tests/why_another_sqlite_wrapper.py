#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试用python自带的sqlite3和SqlAlchemy增删插改的性能差异。
"""

from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import Integer, TEXT
from sqlalchemy.sql import select
import sqlalchemy
import time
import os
import random
import sqlite3

def random_string(length=32):
    s = []
    for _ in range(length):
        s.append(random.choice("abcdefghijklmnopqrstuvwxyz"
                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               "0123456789"))
    return "".join(s)

def reset():
    try:
        os.remove("test.db")
    except:
        pass

def sqlalchemy_vs_sqlite3_bulk_insert():
    """测试在批量插入数据顺利的情况下, Sqlalchemy和sqlite3的速度。
    
    在Inter(R) Xeon(R) CPU E31245 @3.30GHZ, non-SSD, 16GM Ram的情况下,
    
    - Sqlalchemy: 0.015 - 0.042
    - sqlite3: 0.002-0.003
    
    原生API要快10倍左右。
    """
    print("Run sqlalchemy vs sqlite3 bulk insert test...")
    reset()
    
    # sqlalchemy method
    engine = create_engine("sqlite:///test.db", echo=False)
    metadata = MetaData()
    article = Table("article", metadata,
        Column("id", Integer, primary_key=True),
        Column("text", TEXT),
        )
    metadata.create_all(engine)
 
    connect = engine.connect()
    text = "abcdefghijklmnopqrstuvwxyz"
    records = [{"id": i, "text": text} for i in range(1000)]
    ins = article.insert()
     
    st = time.clock()
    connect.execute(ins, records)
    print("sqlalchemy insert takes %.6f sec" % (time.clock()-st))

    reset()
    
    # sqlite3 method
    connect = sqlite3.connect("test.db")
    cursor = connect.cursor()
    
    cursor.execute("CREATE TABLE article (id INTEGER PRIMARY KEY, " 
                   "text TEXT);")
     
    text = "abcdefghijklmnopqrstuvwxyz"
    records = [(i, text) for i in range(1000)]
     
    st = time.clock()
    cursor.executemany("INSERT INTO article VALUES (?,?)", records)
    connect.commit()
    print("sqlite3 insert takes %.6f sec" % (time.clock()-st))
    
    reset()
    
# sqlalchemy_vs_sqlite3_bulk_insert()

def sqlalchemy_vs_sqlite3_bulk_insert_with_exception():
    """在批量插入数据, 且有primary key冲突的情况下, sqlalchemy和sqlite3的效率
    比较。
    
    在有可能发生PRIMARY KEY冲突的情况下, sqlite3原生API有很大的优势。这是因为
    在其他数据库中出现了IntegrityError后, 需要将数据库回滚到上一状态, 才能继续
    进行INSERT。而一旦要回滚, 为了保证插入操作的原子性, 则必须保证每插入一个之后
    都要commit。而sqlite数据库原生API可以不用回滚, 跳过异常后直接插入下一条, 然
    后在最后再进行一次commit。所以, sqlite3中的批量插入要比其他数据库速度上有
    优势。
    """
    print("Run sqlalchemy vs sqlite3 bulk insert with exception test...")
    
    reset()

    # sqlalchemy method
    engine = create_engine("sqlite:///test.db", echo=False)
    metadata = MetaData()
    article = Table("article", metadata,
        Column("id", Integer, primary_key=True),
        Column("text", TEXT),
        )
    metadata.create_all(engine)
 
    connect = engine.connect()
    ins = article.insert()
    
    records = [{"id": i, "text": "Hello World!"} for i in range(100)]
    records = records + [{"id": random.randint(1, 100), 
                          "text": "Hello World!"} for i in range(10)]
    
    st = time.clock()
    for record in records:
        try:
            connect.execute(ins, record)
        except sqlalchemy.exc.IntegrityError:
            pass
    print("sqlalchemy insert takes %.6f sec" % (time.clock()-st))
    connect.close() # 要关闭连接后才能remove掉数据库文件
    
    reset()
    
    # sqlite3 method
    connect = sqlite3.connect("test.db")
    cursor = connect.cursor()
    cursor.execute("CREATE TABLE article (id INTEGER PRIMARY KEY, " 
                   "text TEXT);")
     
    records = [(i, "Hello World!") for i in range(100)] + \
              [(random.randint(1, 100), "Hellow World!") for i in range(10)]
               
    st = time.clock()
    for record in records:
        try:
            cursor.execute("INSERT INTO article VALUES (?,?);", record)
        except sqlite3.IntegrityError:
            pass
    connect.commit()
    print("sqlite3 insert takes %.6f sec" % (time.clock()-st))
    
# sqlalchemy_vs_sqlite3_bulk_insert_with_exception()

def sqlalchemy_vs_sqlite3_select():
    """执行Select命令的效率比较:
    
    由于sqlalchemy默认在选择行的时候会根据column构造一个字典, 使得用户可以用
    row.column_name的方法调用数据。而原生sqlite3返回的是tuple, 所以速度较快。
    而我们希望可以自定义什么时候返回tuple, 什么时候返回dict。这样我们可以更
    灵活的在高性能下, 兼顾可用性。
    """
    print("Run sqlalchemy vs sqlite3 select test...")
    
    reset()

    # sqlalchemy method
    engine = create_engine("sqlite:///test.db", echo=False)
    metadata = MetaData()
    article = Table("article", metadata,
        Column("id", Integer, primary_key=True),
        Column("text", TEXT),
        )
    metadata.create_all(engine)
 
    connect = engine.connect()
    ins = article.insert()
    
    records = [{"id": i, "text": "Hello World!"} for i in range(1000)]
    connect.execute(ins, records)

    st = time.clock() # 速度是原生的sqlite select并打包成字典的 1/3
    for row in connect.execute(select([article])):
        pass
    print("sqlalchemy select takes %.6f sec" % (time.clock()-st))
    
    # sqlite3 method
    connect = sqlite3.connect("test.db")
    cursor = connect.cursor()
     
    st = time.clock() # 速度是通过SqlAlchemy的3倍
    columns = ["_id", "text"]
    for record in cursor.execute("SELECT * FROM article"):
        doc = dict()
        for k, v in zip(columns, record):
            doc[k] = v
    print("sqlite3 select takes %.6f sec" % (time.clock()-st))
    
    reset()
    
# sqlalchemy_vs_sqlite3_select()

engine = create_engine("sqlite:///test.db", echo=False)
metadata = MetaData()
article = Table("article", metadata,
    Column("id", Integer, primary_key=True),
    Column("metadata", TEXT),
    )
metadata.create_all(engine)
print(article.id)
if __name__ == "__main__":
    reset()