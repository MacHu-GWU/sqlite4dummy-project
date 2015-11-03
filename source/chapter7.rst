.. _chapter7:

Chapter7. Delete data
================================================================================

Deletion is simple locate sets of data, and remove them from table. So the syntax of :class:`~sqlite4dummy.schema.Delete` object is very similar to :class:`~sqlite4dummy.schema.Select`.


Example
--------------------------------------------------------------------------------

.. code-block:: python
    
    # ... define a table = Table(...)

    del_obj = table.delete()
    del_obj.where(table.c.column1 >= value1, table.c.column2 <= value2, ...)
    engine.delete(del_obj)


Wants to know more?
--------------------------------------------------------------------------------

Now, I believe you pretty much known everything about sqlite4dummy. For arguments/parameters, more usage and source code, Go :ref:`module index <indice_and_tables>`.