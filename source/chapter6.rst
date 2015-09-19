Chapter6. Insdate (Insert and update)
================================================================================

Suppose you have many csv files to insert into a table, and there's a primary key constrain. At Monday, you pushed everything into the table. But on next day, you received more files to deal with, and the bad news is, part of them are duplicate of Monday's, but the data is newer. Now your logic is to put everything you collected on Tuesday into the table, if primary key conflict raised, update other values (except primary key values) with new data. This is what :meth:`~sqlite4dummy.engine.Sqlite3Engine.insdate_many_record` and :meth:`~sqlite4dummy.engine.Sqlite3Engine.insdate_many_row` designed for.

Of course, you can also use insdate as a regular bulk insert, if there's no primary key constrain involved.


insdate many record
--------------------------------------------------------------------------------

Example::

.. code-block:: python

	ins = table.insert()
	record_list = [(value1, value2, ...) for i in range(10)]
	engine.insdate_many_record(ins, record_list)


insdate many row
--------------------------------------------------------------------------------

Example::

.. code-block:: python

	ins = table.insert()
	row_list = [
		{
			"#column1": value1
			"#column2": value2
			...
		} for i in range(10)
	]
	engine.insdate_many_row(ins, row_list)


Next
--------------------------------------------------------------------------------

OK, we finished Update, let's move to Delete.