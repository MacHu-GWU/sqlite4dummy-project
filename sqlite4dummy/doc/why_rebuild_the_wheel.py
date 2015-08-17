#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
English doc
-----------

We already have an awesome project `Sqlalchemy <http://www.sqlalchemy.org/>`_,
Why I rebuild the wheel again? And why only `Sqlite <https://www.sqlite.org/>`_?

1. In bulk insert operation, sometime we meet primary key conflict. In this scenario, we have to insert records one by one, catch the exception and handle it. Because Sqlalchemy is created to be compatible with most of database system, the way Sqlalchemy handle the exception is rollback. But, sqlite is so special. In sqlite, there's only one writer is allowed at one time, and there's no transaction. That's why sqlite don't need the rollback mechanism. In the sqlite Python generic API, we can simple pass that exception. As result, **the generic API is 50-100 times faster than Sqlalchemy** when there's conflict in bulk insert.

2. Sqlalchemy use Rowproxy to preprocess the data that cursor returns. After that, we can visit value by the column name. But sometime, we actually don't need this feature. A better way is to activate this feature when we need it. That makes **Sqlalchemy is 1.5 to 3 times slower generic API**.

sqlite is an excellent database product. Single file, C API, ultra fast. All these features is what a Data Scientist dream of. It's perfect for analysis on mid size dataset (from 1GB to 1TB). Specifically when tons of selection operation are needed.

Plus, with very small lose of features (missing features are usually useless in analytic work.), the new sqlite4dummy Sqlite API is optimized. The interface is almost a human language. **Which allows non-programming, non-database background statistician, analyist to take advantage of high performance I/O of data from Sqlite.**

At the end, if you don't need transaction, user group and love high performance in batch operations, use sqlite4dummy. Enjoy it!


Chinese doc (中文文档)
---------------------

为什么我们在已经有Sqlalchemy(下称SA)这么优秀的项目的情况下, 我们还要仅仅为sqlite
做一个新的API呢?

SA为了能够兼容所有主流关系数据库, 所以牺牲了一些性能。虽然SA的功能无比强大, 但是
在一些特殊情况下, 并不能给我们带来任何的利益。特别是在数据分析中。

对于数据科学家而言, Sqlite是一个非常适合加速IO的数据库。单文件, C实现, 简单高速,
这些特性都非常适合分析中等大小(1GB - 1TB)的数据集。而Transaction, Session, User
Group这些功能, 我们并不需要。

此外, SA在性能上有两个致命的弱点:

1. SA在执行Select的时候, 调用了一种叫做Rowproxy的机制, 将所有的行打包成字典, 方便
我们进行读取。这一特性我们并不是100%的需要, 而我们完全可以在需要的时候, 再打包
成字典。这使得SA在Select返回大量数据的情况下, 要比原生API慢50%左右。

2. SA在执行Insert的时候, 如果发生了primary key conflict, 由于SA需要兼容所有的数据库,
所以SA使用了rollback机制。而由于sqlite3只支持单线程的write, 所以在处理冲突的时候
要比多线程简单很多, 导致SA的速度在当写入的数据有冲突的时候, 速度要比原生sqlite
API慢几十倍甚至百倍。

这就是我们重新创造一个面向数据分析人员, 而又提供比SA更加简化直观的API的
sqlite4dummy, 让非计算机背景的人员也能轻松愉快的使用sqlite数据库带来的极大便利。
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
    res = []
    for _ in range(length):
        res.append(random.choice("abcdefghijklmnopqrstuvwxyz"
                               "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                               "0123456789"))
    return "".join(res)

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
    
    connect.close()
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
    
    
sqlalchemy_vs_sqlite3_bulk_insert()

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

if __name__ == "__main__":
    reset()