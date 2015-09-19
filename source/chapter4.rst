.. _chapter4:

Chapter4. More about Insert
================================================================================

Except :meth:`~sqlite4dummy.engine.Sqlite3Engine.insert_record`, :class:`~sqlite4dummy.engine.Sqlite3Engine` also provides:

- :meth:`~sqlite4dummy.engine.Sqlite3Engine.insert_row`
- :meth:`~sqlite4dummy.engine.Sqlite3Engine.insert_many_record`
- :meth:`~sqlite4dummy.engine.Sqlite3Engine.insert_many_row`
- :meth:`~sqlite4dummy.engine.Sqlite3Engine.insert_record_stream`
- :meth:`~sqlite4dummy.engine.Sqlite3Engine.insert_row_stream`

Before you proceed, first you should understand the :mod:`Row Proxy<sqlite4dummy.row>`, which offers you the ability to visit it's value and define database row data like a Python dict.

- :ref:`For English doc<row-en>`
- :ref:`For Chinese doc<row-cn>`

Once you get understand :mod:`Row Proxy<sqlite4dummy.row>`, let's see more features about Insert.

Insert a Row object
--------------------------------------------------------------------------------

Insert one Row object at a time. If conflict with constrain, raise Errors.

.. code-block:: python
	
	... define a table = Table(...)

	ins = table.insert()
	row = Row.from_dict({
		"#column1": value1
		"#column2": value2
		...
	})
	engine.insert_row(ins, row)


.. _insert-many-record-tuple:

Insert many record tuple
--------------------------------------------------------------------------------

Insert list of records, automatically skip Exceptions.

.. code-block:: python

	ins = table.insert()
	record_list = [(value1, value2, ...) for i in range(10)]
	engine.insert_many_record(ins, record_list)


.. _insert-many-row-object:

Insert many Row object
--------------------------------------------------------------------------------

Insert list of Row objects, automatically skip Exceptions.

.. code-block:: python

	ins = table.insert()
	row_list = [
		{
			"#column1": value1
			"#column2": value2
			...
		} for i in range(10)
	]
	engine.insert_many_row(ins, row_list)


Insert many record tuple in a generator stream
--------------------------------------------------------------------------------

Similar to :ref:`insert-many-record-tuple`, but work with a record tuple generator.

.. code-block:: python

	def record_generator()
		record_list = [(value1, value2, ...) for i in range(10)]
		for record in record_list:
			yield record

	ins = table.insert()
	engine.insert_many_row(ins, record_generator())


Insert many Row object in a generator stream
--------------------------------------------------------------------------------

Similar to :ref:`insert-many-row-object`, but work with a Raw object generator.

.. code-block:: python

	def row_generator()
		row_list = [
			{
				"#column1": value1
				"#column2": value2
				...
			} for i in range(10)
		]
		for row in row_list:
			yield row

	ins = table.insert()
	engine.insert_many_row(ins, row_generator())


Next
--------------------------------------------------------------------------------

OK, we finished Insert, let's move to Select.

:ref:`Next Chapter <chapter5>`