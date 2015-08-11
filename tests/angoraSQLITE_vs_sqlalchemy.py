import angora.SQLITE as sqlite
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData
from sqlalchemy import Integer, TEXT, PickleType
from sqlalchemy.sql import select
import sqlalchemy
import time
import os
import random

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
    
reset()

engine = create_engine("sqlite:///test.db", echo=False)
metadata = MetaData()
article = Table("article", metadata,
    Column("id", Integer, primary_key=True),
    Column("text", TEXT),
    Column("content", PickleType)
    )
metadata.create_all(engine)

connect = engine.connect()
text = "abcdefghijklmnopqrstuvwxyz"
records = [{"id": i, "text": text, "content": [random_string(8), random_string(8)]} for i in range(1000)]
ins = article.insert()
connect.execute(ins, records)

st = time.clock()
for row in connect.execute(select([article])):
    pass
print("sqlalchemy select takes %.6f sec" % (time.clock()-st))

engine = sqlite.Sqlite3Engine("test.db")
metadata = sqlite.MetaData()
metadata.reflect(engine)

article = metadata.get_table("article")

st = time.clock()
for record in engine.select(sqlite.Select(article.all)):
    pass
print("sqlite select takes %.6f sec" % (time.clock()-st))