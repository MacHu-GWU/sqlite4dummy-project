#!/usr/bin/env python
# -*- coding: utf-8 -*-

if __name__ == "__main__":
    from sql4dummy.schema import Column, Table, MetaData, Sqlite3Engine
    from sql4dummy.dtype import dtype
    from datetime import datetime, date
    from pprint import pprint as ppt
    import unittest
    
    class MetaDataUnittest(unittest.TestCase):
        def setUp(self):
            self.engine = Sqlite3Engine(":memory:", autocommit=False)
            self.metadata = MetaData()
            self.test = Table("test", self.metadata,
                Column("_string", dtype.TEXT, primary_key=True),
                Column("_int_with_default", dtype.INTEGER, default=1),
                Column("_float_with_default", dtype.REAL, default=3.14),
                Column("_byte_with_default", dtype.BLOB, default=b"8e01ad49"),
                Column("_date_with_default", dtype.DATE, default=date(2000, 1, 1)),
                Column("_datetime_with_default", dtype.DATETIME, default=datetime(2015, 12, 31, 8, 30, 17, 123)),
                Column("_pickle_with_default", dtype.PICKLETYPE, default=[1, 2, 3]),
                Column("_int", dtype.INTEGER),
                Column("_float", dtype.REAL),
                Column("_byte", dtype.BLOB),
                Column("_date", dtype.DATE),
                Column("_datetime", dtype.DATETIME),
                Column("_pickle", dtype.PICKLETYPE),
                )
            self.metadata.create_all(self.engine)
#             ppt(self.engine.execute("PRAGMA table_info(test);").fetchall())
            
        def test_str_repr(self):
#             print(self.metadata)
#             print(repr(self.metadata))
            pass
        
        def test_get_table(self):
            """测试MetaData.get_table(table_name)方法是否能正确获得Table。
            """
            self.assertEqual(self.metadata.get_table("test"), self.test)
            self.assertRaises(KeyError,
                              self.metadata.get_table, "not_existsing_table")
            
        def test_reflect(self):
            """测试MetaData.reflect(engine)是否能正确解析出Table, Column的
            metadata, 并且解析出default值。
            """
            second_metadata = MetaData()
            second_metadata.reflect(self.engine, 
                                    pickletype_columns=[
                                        "test._pickle_with_default",
                                        "test._pickle",
                                        ])
            self.assertEqual(second_metadata.get_table("test").\
                             c._int_with_default.default, 1)
            self.assertEqual(second_metadata.get_table("test").\
                             c._float_with_default.default, 3.14)
            self.assertEqual(second_metadata.get_table("test").\
                             c._byte_with_default.default, b"8e01ad49")
            self.assertEqual(second_metadata.get_table("test").\
                             c._date_with_default.default, date(2000, 1, 1))
            self.assertEqual(second_metadata.get_table("test").\
                             c._datetime_with_default.default, 
                             datetime(2015, 12, 31, 8, 30, 17, 123))
            self.assertEqual(second_metadata.get_table("test").\
                             c._pickle_with_default.default, [1, 2, 3])
            
    unittest.main()