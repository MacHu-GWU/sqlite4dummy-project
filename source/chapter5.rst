Chapter5. More about Select
================================================================================

Select statement is the most complicate syntax in SQL language. But the objected oriented style API helps user focus on what they want.

There are lots of optional Clause and generic Sql function can apply to a :class:`~sqlite4dummy.schema.Select` object. Here's the list of them:

- :meth:`~sqlite4dummy.schema.Select.where`
- :meth:`~sqlite4dummy.schema.Select.order_by`
- :meth:`~sqlite4dummy.schema.Select.limit`
- :meth:`~sqlite4dummy.schema.Select.offset`
- :meth:`~sqlite4dummy.schema.Select.distinct`
- :meth:`~sqlite4dummy.schema.Select.select_from`


SQL Clause
--------------------------------------------------------------------------------

SQL Clause refers to Select SQL statement components.


`WHERE Clause <http://www.w3schools.com/sql/sql_where.asp>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example::

.. code-block:: python

	Select([table.c.column1, table.c.column2, ...]).\
		where(table.c.column1 == 1, table.c.column2 >= 2)

For all supported arguments, go :meth:`~sqlite4dummy.schema.Select.where`


`ORDER BY Clause <http://www.w3schools.com/sql/sql_orderby.asp>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example:

.. code-block:: python

	Select(table.all).order_by(table.c.column1, table.c.column2.desc())


For all supported arguments, go :meth:`~sqlite4dummy.schema.Select.order_by`


`LIMIT Clause <http://www.w3schools.com/sql/sql_top.asp>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example:

.. code-block:: python

	Select(table.all).limit(20) # only return first 20 matched records


OFFSET Clause
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example:

.. code-block:: python

	Select(table.all).limit(20).offset(100) # skip first 100, fetch 20


`DISTINCT Clause <http://www.w3schools.com/sql/sql_distinct.asp>`_
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Example:

.. code-block:: python

	Select([talbe.c.column1, table.c.column2]).distinct()


SELECT FROM Clause
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

SELECT FROM is actually execute a selection on results of another selection.

.. code-block:: python

	Select(...).select_from(Select(...))


Execute Selection
--------------------------------------------------------------------------------

Suppose table's data is:

.. code-block:: python

	columns = ["_id", "_string", "_list"]
	data = [
		[1, "a", [1, 2, 3]],
		[2, "b", [1, 2, 3]],
		[3, "c", [1, 2, 3]],
	]


Return Record
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

	sel = Select(...)
	for record in engine.select(sel): # or engine.select_record(sel)
		print(record)

	Print Screen...

	[1, 'a', [1, 2, 3]]
	[2, 'b', [1, 2, 3]]
	[3, 'c', [1, 2, 3]]

For more information go: :meth:`~sqlite4dummy.engine.Sqlite3Engine.select`


Return Row
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

	sel = Select(...)
	for row in engine.select_row(sel):
		print(row.to_dict()) # you can make use of Row proxy, like row._id, row["_string"]

	Print Screen...

	OrderedDict([('_id', 1), ('_string', "a"), ('_list', [1, 2, 3])])
	OrderedDict([('_id', 2), ('_string', "b"), ('_list', [1, 2, 3])])
	OrderedDict([('_id', 3), ('_string', "c"), ('_list', [1, 2, 3])])

For more information go: :meth:`~sqlite4dummy.engine.Sqlite3Engine.select_row`


Return dict
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

	sel = Select(...)
	res = engine.select_dict(sel):
	print(res)

	Print Screen...
	{
		'_id': [1, 2, 3]
		'_string': ['a', 'b', 'c']
		'_list': [[1, 2, 3], [1, 2, 3], [1, 2, 3]]
	}

For more information go: :meth:`~sqlite4dummy.engine.Sqlite3Engine.select_dict`


Return pandas.DataFrame
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`pandas.DataFrame <http://pandas.pydata.org/pandas-docs/stable/generated/pandas.DataFrame.html>`_ is a column oriented, indexed 2d-array data structure. It's the top choice for analytic job in Python community.

.. code-block:: python

	sel = Select(...)
	res = engine.select_df(sel):
	print(res)

	Print Screen...

	   _id      _list _string
	0    1  [1, 2, 3]       a
	1    2  [1, 2, 3]       b
	2    3  [1, 2, 3]       c

For more information go: :meth:`~sqlite4dummy.engine.Sqlite3Engine.select_df`


Next
--------------------------------------------------------------------------------

OK, we finished Select, let's move to Update.