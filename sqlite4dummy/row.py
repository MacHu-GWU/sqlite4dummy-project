#!/usr/bin/env python
# -*- coding: utf-8 -*-

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
        return str(self.values)
    
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
        Row.data进行操作, 而是通过:meth:`Row[column_name]<Row.__getitem__>`
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
