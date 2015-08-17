#!/usr/bin/env python
# -*- coding: utf-8 -*-

from sqlite4dummy import *
from datetime import date
import os

def reset():
    try:
        os.remove("test.db")
    except:
        pass
    
reset()

metadata = MetaData()
employee = Table("employee", metadata,
    Column("_id", dtype.INTEGER, primary_key=True),
    Column("name", dtype.TEXT, nullable=False),
    Column("gender", dtype.TEXT, default="unknown"),
    Column("height", dtype.REAL),
    Column("hire_date", dtype.DATE),
    Column("profile", dtype.PICKLETYPE), # dict json
    )
engine = Sqlite3Engine("test.db", autocommit=False) # autocommit default True
metadata.create_all(engine)

ins = employee.insert()
record = (1, "John David", "male", 174.5, date(2000, 1, 1), 
    {
        "title": "System engineer",
        "salary": 56000,
        "unit": "USD",
    },
)
engine.insert_record(ins, record)
engine.commit()

ins = employee.insert()
row = Row.from_dict({
    "_id": 2,
    "name": "Black Johnson",
    "height": 185.5,
    "hire_date": date(2007, 5, 15),
    "profile": {
        "title": "Marketing specialist",
        "salary": 47000,
        "unit": "USD",
        "location": "New York",
        "memo": "A very nice person",
    }
})

engine.insert_row(ins, row)
engine.commit()
    
# engine.prt_all(employee)

del_obj = employee.delete()
del_obj.where(employee.c.gender=="unknown")
engine.delete(del_obj)


engine.prt_all(employee)
if __name__ == "__main__":
    pass