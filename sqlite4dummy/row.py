#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
.. _row-en:

English Doc
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

By default the cursor returns record tuple. Row object is a package of record
data, which provides two more way of visiting values by it's column name:

1. ``Row.column_name``
2. ``Row[column_name]``

Create a Row object, column order are preserved:

.. code-block:: python

    >>> row = Row(columns=["c1", "c2"], values=[1, 2])
    >>> print(row)
    (1, 2)
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 2])

Create a Row object from a dictionary, if it is an OrderedDict, then column 
order are preserved. Otherwise, the order is random. But it doens't matter for
executing Insert, Update:

.. code-block:: python

    >>> from collections import OrderedDict
    >>> d = OrderedDict([("c1", 1), ("c2", 2)])
    >>> row = Row.from_dict(d)
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 2])
    
Visit value:

.. code-block:: python

    >>> row.values
    (1, 2)
    >>> row.c1
    1
    >>> row["c2"]
    2
    
Get dictionary view of a Row object::

    >>> row.data
    OrderedDict([('c1', 1), ('c2', 2)])
    
Edit Row:

.. code-block:: python
    
    # correct way
    >>> row["c2"] = 20
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 20])
    
    # wrong way
    >>> row.c2 = 1000
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 20])

Create a copy of Row data. Avoid changing the Row object it self. use
:meth:`Row.to_dict`:

.. code-block:: python

    # 这样做对data进行的任何修改不会影响到原来的Row对象
    >>> data = row.to_dict()
    >>> data
    OrderedDict([('c1', 1), ('c2', 2)])
    
Sure it also support ``in`` and ``==`` keyword:

.. code-block:: python

    >>> "c1" in row
    True
    
    >>> row == Row(columns=["c1", "c2"], values=[1, 2])
    True


.. _row-cn:

Chinese Doc (中文文档)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Row模块用于包装游标(Cursor)返回的数据。默认条件下游标返回的是元组(tuple)。而对 
元组中的值进行访问只能通过位置索引, 并不是很方便, 也不方便对其编程。Row提供了     
通过列名对值进行访问的方法, 一共有两种形式:

1. ``Row.column_name``
2. ``Row[column_name]``

创建一个Row对象:

.. code-block:: python

    # 通过columns, values初始化, 能够保存下顺序信息
    >>> row = Row(columns=["c1", "c2"], values=[1, 2])
    >>> print(row)
    (1, 2)
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 2])

从字典创建一个Row对象:

.. code-block:: python

    # 通过有序字典初始化Row对象, 则能保存下顺序信息。如果使用原生Python dict
    # 则无法保证顺序。但是在用于使用Row执行Insert, Update的时候并不影响。
    >>> from collections import OrderedDict
    >>> d = OrderedDict([("c1", 1), ("c2", 2)])
    >>> row = Row.from_dict(d)
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 2])
    
对Row的值进行访问:

.. code-block:: python

    >>> row.values
    (1, 2)
    >>> row.c1
    1
    >>> row["c2"]
    2
    
取得Row的字典视图, 注意, 此方法用于只读::

    >>> row.data
    OrderedDict([('c1', 1), ('c2', 2)])
    
对Row的值进行修改:

.. code-block:: python
    
    # 使用索引修改Row的值
    >>> row["c2"] = 20
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 20])
    
    # 注意! 这是错误的方法。这样相当于给Row新增了一个属性Row.c2, 而不是对
    # Row["c2"]的值进行了修改。这样以后就无法使用Row.c2正确滴访问Row["c2"]的值了
    >>> row.c2 = 1000
    >>> row
    Row(columns=['c1', 'c2'], values=[1, 20])
    
如果要在其他地方使用到Row中的数据, 而且会涉及修改操作, 则使用
:meth:`Row.to_dict` 方法:

.. code-block:: python

    # 这样做对data进行的任何修改不会影响到原来的Row对象
    >>> data = row.to_dict()
    >>> data
    OrderedDict([('c1', 1), ('c2', 2)])
    
当然我们还提供了``in``, ``==``关键字的支持:

.. code-block:: python

    >>> "c1" in row
    True
    >>> row == Row(columns=["c1", "c2"], values=[1, 2])
    True
    
class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from collections import OrderedDict
import copy

class Row():
    """An abstract class of single cursor row.
    
    Basically, it behave like an ``OrderedDict``. You can visit column's value::
    
        >>> row = Row(columns=("_id", "value"), values=("a", 1))
        >>> row._id
        "a"
        
        >>> row["value]
        1
    
    **中文文档**
    
    数据表中的行数据类。 可以使用索引Row[column_name]或是属性Row.column_name的
    方式对值进行访问。
    """
    
    def __init__(self, columns, values):
        """Row object are constructed by columns tuple and values tuple.
        """
        self.columns = columns
        self.values = values
        self.dict_view = None
    
    @staticmethod
    def from_dict(dictionary):
        """Create a Row object from a Python dictionary.
        
        ::
        
            >>> from collections import OrderedDict
            >>> row = Row.from_dict(OrderedDict(("_id", "value"), ("a", 1)))
            >>> row
            ('a', 1)
        
        **中文文档**
        
        根据字典生成Row对象。最好使用OrderedDict, 不然columns, values的顺序
        无法保证每次都一致。但在你不需要调用Row.columns或Row.values的时候,
        没有任何影响。
        """
        return Row(tuple(dictionary.keys()), tuple(dictionary.values()))

    def __str__(self):
        return str(tuple(self.values))
    
    def __repr__(self):
        return "Row(columns=%s, values=%s)" % (self.columns, self.values)
    
    def _create_dict_view(self):
        """create a dictionary view of {column: value}

        **中文文档**
        
        在初始化Row对象时, 仅储存columns, values两个tuple。这样可以在不需要
        ``dict_view``时节约不必要的开销。在调用需要``dict_view``的方法时, 再临时
        创建即可, 从而节约了计算量
        """
        self.dict_view = OrderedDict()
        for column_name, value in zip(self.columns, self.values):
            self.dict_view[column_name] = value
    
    @property
    def data(self):
        """Return dict view of this Row.
        
        **中文文档**
        
        返回Row的字典视图, 对其修改等于是修改Row对象本身。所以尽量不要直接对
        Row.data进行重新赋值操作。
        """
        if not self.dict_view:
            self._create_dict_view()
        return self.dict_view
    
    def to_dict(self):
        """Convert Row to a new Python dict.
        
        **中文文档**
        
        将Row转化成一个新字典, 对新字典的任何修改不会影响Row.dict_view本身。 
        """
        if not self.dict_view:
            self._create_dict_view()
        return copy.deepcopy(self.dict_view)
    
    def __getitem__(self, column_name):
        """Get column's value. Use index syntax.
        
        **中文文档**
        
        用切片语法取得某个column的值。
        """
        try:
            return self.dict_view[column_name]
        except:
            self._create_dict_view()
            return self.dict_view[column_name]
    
    def __setitem__(self, column_name, value):
        """Edit columns' value, use index syntax.
        
        **中文文档**
        
        修改某个column的值。
        """
        try:
            self.dict_view[column_name] = value
        except:
            self._create_dict_view()
            self.dict_view[column_name] = value

        self.columns = tuple(self.dict_view.keys())
        self.values = tuple(self.dict_view.values())
        
    def __getattr__(self, column_name):
        """Get column's value. Use attribute syntax.

        **中文文档**
        
        用属性语法取得某个column的值。
        """
        try:
            return self.dict_view[column_name]
        except:
            self._create_dict_view()
            return self.dict_view[column_name]

    def __contains__(self, column_name):
        return column_name in self.columns
            
    def items(self):
        """Return a list of tuples, each tuple containing a key/value pair.
        
        **中文文档**
        
        对key, value进行访问的循环器。
        """
        for column_name, value in zip(self.columns, self.values):
            yield column_name, value
    
    def __eq__(self, other):
        if not self.dict_view:
            self._create_dict_view()
        if not other.dict_view:
            other._create_dict_view()

        return dict(self.dict_view) == dict(other.dict_view)
    
if __name__ == "__main__":
    import unittest
    
    class RowUnittest(unittest.TestCase):
        def setUp(self):
            self.row1 = Row(("_id", "value"), ("a", 1))
            self.row2 = Row.from_dict({"_id": "a", "value": 1})
        
        def test_display(self):
            self.assertEqual(str(self.row1), "('a', 1)")
            self.assertEqual(repr(self.row1), 
                             "Row(columns=('_id', 'value'), values=('a', 1))")
            
        def test_dict_view(self):
            self.assertDictEqual(self.row1.data, 
                                 OrderedDict([('_id', 'a'), ('value', 1)]))
            self.assertDictEqual(self.row1.to_dict(), 
                                 OrderedDict([('_id', 'a'), ('value', 1)]))
            
        def test_get_column_value(self):
            self.assertEqual(self.row1._id, "a")
            self.assertEqual(self.row1.value, 1)
            self.assertEqual(self.row2["_id"], "a")
            self.assertEqual(self.row2["value"], 1)
        
        def test_contain(self):
            self.assertTrue("_id" in self.row1)
            self.assertFalse("name" in self.row2)
        
        def test_items(self):
            self.assertListEqual(list(self.row1.items()), 
                                 [("_id", "a"), ("value", 1)])
            
        def test_eq(self):
            self.assertEqual(self.row1, self.row2)
        
        def test_edit_value(self):
            row = Row(columns=["_id", "value"], values=["a", 1])
            row["_id"] = "b"
            self.assertEqual(row._id, "b")

    unittest.main()
