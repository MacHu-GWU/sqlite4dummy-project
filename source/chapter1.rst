.. _quick-start:

Chapter1. Quick Start
=====================

First, let's connect to a database, create a table, then do some CRUD (CREATE, READ, UPDATE, DELETE).

Connect to database, Create Table
---------------------------------------------------------------------------------------------------

.. code-block:: python

	from sqlite4dummy import *

	metadata = MetaData()
	employee = Table("employee", metadata,
	    Column("_id", dtype.INTEGER, primary_key=True),
	    Column("name", dtype.TEXT, nullable=False),
	    Column("gender", dtype.TEXT, default="unknown"),
	    Column("height", dtype.REAL),
	    Column("hire_date", dtype.DATE),
	    Column("profile", dtype.PICKLETYPE), # any pickable python type
	    )
	engine = Sqlite3Engine("test.db", autocommit=False) # autocommit default True
	metadata.create_all(engine)


Insert some data
---------------------------------------------------------------------------------------------------

There's two ways: 

1. insert record tuple and 

.. code-block:: python
	
	from datetime import date

	ins = employee.insert() # initiate Insert object
	record = (1, "John David", "male", 174.5, date(2000, 1, 1), 
		{ # json data (PICKLETYPE)
			"title": "System engineer",
			"salary": 56000,
			"unit": "USD",
		},
	)
	engine.insert_record(ins, record)
	engine.commit()

	engine.prt_all(employee) # print what we have inserted

2. insert Row object

.. code-block:: python
	
	from datetime import date

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

	engine.prt_all(employee) # print what we have inserted


Select data
---------------------------------------------------------------------------------------------------

select all

.. code-block:: python

	sel = Select(employee.all) # Create a Select object
	for record in engine.select(sel):
		print(record)

select columns

.. code-block:: python

	sel = Select([employee.c._id, employee.c.name]])
	for record in engine.select(sel):
		print(record)

where clause

.. code-block:: python

	sel = Select(employee.all).where(employee.c._id==1)
	for record in engine.select(sel):
		print(record)


Update data
---------------------------------------------------------------------------------------------------

.. code-block:: python

	upd = employee.update() # Update object is constructed via Table.update
	upd.values(hire_date=date(2010, 12, 17)).where(employee.c.gender=="unknown")
	engine.update(upd)
	engine.commit()

	engine.prt_all(employee) # print what we have updated


Delete data
---------------------------------------------------------------------------------------------------

.. code-block:: python

	del_obj = employee.delete() # Create a Delete object
	del_obj.where(employee.c.gender=="unknown")
	engine.delete(del_obj)

	engine.prt_all(employee) # print data after we deleted some.

Next
---------------------------------------------------------------------------------------------------

Now we are capable to do the basic things with database. Next, let's see how we do lower level operation on database, table and index.