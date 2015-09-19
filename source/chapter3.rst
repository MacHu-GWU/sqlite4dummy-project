Chapter3. Sqlite Engine
================================================================================

Any operation has to be done via :class:`~sqlite4dummy.engine.Sqlite3Engine`. The engine is the most top class built on top of Column, Table, Index, MetaData, Insert, Select, Update and Delete.


Connection and Cursor
---------------------------------------------------------------------------------------------------

``sqlite3.Connect`` and ``sqlite3.Cursor`` instance are hold as attributes of :class:`~sqlite4dummy.engine.Sqlite3Engine`.

.. code-block:: python

	engine = Sqlite3Engine(":memory:")
	engine.connect # sqlite3.Connect instance
	engine.cursor # sqlite3.Cursor instance

You can call :meth:`sqlite4dummy.engine.Sqlite3Engine.execute` (some time :meth:`sqlite4dummy.engine.Sqlite3Engine.executemany`) to execute arbitrary Sql command as you do in generic sqlite3 Python API.


Understand auto commit
---------------------------------------------------------------------------------------------------

Like other relational database system, in sqlite3 you have to perform connect.commit() to make your change such as Insert and Update taken effects.

By default, the autocommit is on. But if you need better performance, you could disable autocommit and manually do commit when you need. For example:

Do this:

.. code-block:: python

	engine = Sqlite3Engine("test.db", autocommit=False)

Or:

.. code-block:: python

	engine = Sqlite3Engine("test.db")
	engine.set_autocommit(False)

Manually do commit:

.. code-block:: python

	engine.commit()


Vanilla method
---------------------------------------------------------------------------------------------------

sqlite4dummy minimize some frequently-used commands to let user's doing complex things in small piece of codes. Basically, it's just a syntax wrapper. But it's really helpful.

:meth:`~sqlite4dummy.engine.Sqlite3Engine.howmany`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

return how many records in a table.

.. code-block:: python

	... define a table = Table(...)

	count = engine.howmany(table) # SELECT COUNT(*) FROM (SELECT * FROM table)


:meth:`~sqlite4dummy.engine.Sqlite3Engine.tabulate`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

return all records packed in a list in a table.

.. code-block:: python

	data = engine.tabulate(table) # list of record


:meth:`~sqlite4dummy.engine.Sqlite3Engine.dictize`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

return all records in a column oriented view in a table.

.. code-block:: python

	data = engine.dictize(table)
	data["#column_name"] # get all column data


:meth:`~sqlite4dummy.engine.Sqlite3Engine.to_df`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

return all records in pandas.DataFrame view in a table. `pandas <http://pandas.pydata.org/>`_ are required.

.. code-block:: python

	df = engine.to_df(table)
	df["#column_name"] # get all column data


:meth:`~sqlite4dummy.engine.Sqlite3Engine.prt_all`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

print all records in a table.

.. code-block:: python

	engine.prt_all(table) # this should print all data in a table


:meth:`~sqlite4dummy.engine.Sqlite3Engine.remove_all`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

remove all data in a table by Table object (or by table name).

.. code-block:: python

	engine.remove_all(table)
	engine.prt_all(table) # this should print no data.


Next
--------------------------------------------------------------------------------

From next chapter, I gonna introduce more features about Insert, Select, Update and Delete.