import pickle
import time
import random
from collections import OrderedDict
class Column():
    def __init__(self, name, is_pk):
        self.name = name
        self.is_pk = is_pk
        
        
columns = [
    Column("a", False),
    Column("b", False),
    Column("c", True),
    ]

tables = dict()
tables["columns"] = OrderedDict()
for c in columns:
    tables["columns"][c.name] = c

data = [(
         random.randint(1, 1024), 
         "Hello World", 
         {random.randint(1, 1024): random.randint(1, 1024) for j in range(10)}, 
        ) for i in range(10000)]

st = time.clock()

for record in data:
    new_record = list()
    for column, value in zip(tables["columns"].values(), record):
        if column.is_pk:
            if value:
                new_record.append(pickle.dumps(value))
        new_record.append(value)
        
print(time.clock() - st)

def process(record):
    record = list(record)
    record[2] = pickle.dumps(record[2])
    return record

st = time.clock()

new_data = map(process, data)
for record in new_data:
    pass
print(time.clock() - st)    