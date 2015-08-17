Chapter2. Database Level Method
===============================

First let's get create some test data sets:

.. code-block:: python

	from sqlite4dummy import *

	metadata = MetaData()
	number = Table("number", metadata,
		Column("_id", dtype.INTEGER, primary_key=True),
		Column("value", dtype.REAL),
		)
	article = Table("article", metadata,
		Column("_id", dtype.INTEGER, primary_key=True),
		Column("content", dtype.TEXT),
		)
	engine = Sqlite3Engine("test.db") # use autocommit default True
	metadata.create_all(engine)


Drop table
~~~~~~~~~~

drop one table:

.. code-block:: python
	
	print(engine.all_tablename) # before: ['number', 'article']
	article.drop(engine)
	print(engine.all_tablename) # after: ['number']

drop all table:

.. code-block:: python
	
	print(engine.all_tablename) # before: ['number']
	metadata.drop_all(engine)
	print(engine.all_tablename) # after: []


Create index
~~~~~~~~~~~~

.. code-block:: python
	
	number_value_index = Index("number_value_index", 
	                           metadata, number.c.value.desc())
	number_value_index.create(engine)

	article_content_index = Index("article_content_index", 
	                              metadata, article.c.content) # default ascending
	article_content_index.create(engine)

	print(engine.all_indexname) # ['number_value_index', 'article_content_index']


Drop index
~~~~~~~~~~

.. code-block:: python
	
	print(engine.all_indexname) # ['number_value_index', 'article_content_index']
	number_value_index.drop(engine)
	print(engine.all_indexname) # [article_content_index']

	print(engine.all_indexname) # [article_content_index']
	article_content_index.drop(engine)
	print(engine.all_indexname) # []


Reflect metadata from existsing database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Let's create table and index using built-in Python Sqlite3 API.

.. code-block:: python

	import sqlite3
	connect = sqlite3.connect("test.db")
	cursor = connect.cursor()
	cursor.execute("CREATE TABLE number (_id INTEGER PRIMARY KEY, value REAL)")
	cursor.execute("CREATE TABLE article (_id INTEGER PRIMARY KEY, content TEXT)")
	cursor.execute("CREATE INDEX number_value_index ON number (value)")
	cursor.execute("CREATE INDEX article_content_index ON article (content DESC)")
	connect.commit()
	connect.close()

Then we switch sqlite4dummy.

.. code-block:: python

	from sqlite4dummy import *

	metadata = MetaData()
	engine = Sqlite3Engine("test.db")
	metadata.reflect(engine)
	print(metadata)

	Screen...

	Binded with test.db
	Table('number', MetaData(), 
		Column('_id', dtype.INTEGER, nullable=True, default=None, primary_key=True),
		Column('value', dtype.REAL, nullable=True, default=None, primary_key=False)
		)
	Table('article', MetaData(), 
		Column('_id', dtype.INTEGER, nullable=True, default=None, primary_key=True),
		Column('content', dtype.TEXT, nullable=True, default=None, primary_key=False)
		)
	Index('number_value_index', MetaData(), 
		'value'
		unique='number',
		table_name=False,
		)
	Index('article_content_index', MetaData(), 
		'content DESC'
		unique='article',
		table_name=False,
		)

Now you can easily play with Table, Index, Column object like this:

.. code-block:: python

	# access Table instance
	number = metadata.get_table("number")
	article = metadata.get_table("article")

	# access Column instance
	value = number.c.value
	content = article.c.content

	# access Index instance
	number_value_index = metadata.get_index("number_value_index")
	article_content_index = metadata.get_index("article_content_index")
