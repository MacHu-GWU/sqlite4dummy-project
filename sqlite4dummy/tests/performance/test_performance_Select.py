#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
本测试模块用于测试sqlite4dummy在Select时与sqlalchemy的性能比较。


class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

from __future__ import print_function, unicode_literals
from sqlite4dummy import *
from sqlite4dummy.tests.basetest import (
    AdvanceUnittestHasData, AdvanceUnittestNoData, DB_FILE)
import sqlalchemy
import time
import unittest

class SelectPerformanceUnittest(AdvanceUnittestHasData):
    """
     
    **中文文档**
     
    测试分别在返回record, row以及结果中 有/无 pickletype的情况下, sqlite4dummy
    的性能是否高于sqlalchemy。
     
    1. record + has pickle type, 两者不相上下
    2. record + no pickle type, sqlite4dummy明显胜出, 用时是另一个的1/2
    3. row + has pickle type, sqlalchemy胜出, 用时是另一个的2/3
    4. row + no pickle type, sqlite4dummy胜出, 用时是另一个的2/3
    """
    def test_select_record_has_pk(self):
        # start sqlalchemy engine
        sa_metadata = sqlalchemy.MetaData()
        sa_table = sqlalchemy.Table("has_pk", sa_metadata,
            sqlalchemy.Column("_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("_pickle", sqlalchemy.PickleType),
        )
        sa_engine = sqlalchemy.create_engine("sqlite:///%s" % DB_FILE)
        sa_metadata.create_all(sa_engine)
         
        print("\nSelect record HAS pickle type")
        
        # sqlite4dummy, 0.0065
        st = time.clock()
        for record in self.engine.select(Select(self.has_pk.all)):
            pass
        speed_sqlite4dummy = time.clock() - st
        print("sqlite4dummy elapse %.6f seconds." % speed_sqlite4dummy)
        print("%s item returns." % 
              len(list(self.engine.execute("SELECT * FROM has_pk"))))
        
        # sqlalchemy, 0.0055
        st = time.clock()
        for record in sa_engine.execute(sqlalchemy.sql.select([sa_table])):
            pass
        speed_sqlalchemy = time.clock() - st
        print("sqlalchemy elapse %.6f seconds." % speed_sqlalchemy)
        print("%s item returns." %
              len(list(self.engine.execute("SELECT * FROM has_pk"))))
         
    def test_select_record_non_pk(self):
        # start sqlalchemy engine
        sa_metadata = sqlalchemy.MetaData()
        sa_table = sqlalchemy.Table("non_pk", sa_metadata,
            sqlalchemy.Column("_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("_str", sqlalchemy.TEXT),
        )
        sa_engine = sqlalchemy.create_engine("sqlite:///%s" % DB_FILE)
        sa_metadata.create_all(sa_engine)
        
        print("\nSelect record NON pickle type")
        
        # sqlite4dummy, 0.0025
        st = time.clock()
        for record in self.engine.select(Select(self.non_pk.all)):
            pass
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print("%s item returns." % 
              len(list(self.engine.execute("SELECT * FROM non_pk"))))
        
        # sqlalchemy, 0.0055
        st = time.clock()
        for record in sa_engine.execute(sqlalchemy.sql.select([sa_table])):
            pass
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print("%s item returns." %
              len(list(self.engine.execute("SELECT * FROM non_pk"))))
   
    def test_select_row_has_pk(self):
        # start sqlalchemy engine
        sa_metadata = sqlalchemy.MetaData()
        sa_table = sqlalchemy.Table("has_pk", sa_metadata,
            sqlalchemy.Column("_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("_pickle", sqlalchemy.PickleType),
        )
        sa_engine = sqlalchemy.create_engine("sqlite:///%s" % DB_FILE)
        sa_metadata.create_all(sa_engine)
        
        print("\nSelect row HAS pickle type")
        
        # sqlite4dummy, 0.0077
        st = time.clock()
        for row in self.engine.select_row(Select(self.has_pk.all)):
            pass
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print("%s item returns." % 
              len(list(self.engine.execute("SELECT * FROM has_pk"))))
        
        # sqlalchemy, 0.0057
        st = time.clock()
        for row in sa_engine.execute(sqlalchemy.sql.select([sa_table])):
            pass
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print("%s item returns." %
              len(list(self.engine.execute("SELECT * FROM has_pk"))))
   
    def test_select_row_no_pk(self):
        # start sqlalchemy engine
        sa_metadata = sqlalchemy.MetaData()
        sa_table = sqlalchemy.Table("non_pk", sa_metadata,
            sqlalchemy.Column("_id", sqlalchemy.Integer, primary_key=True),
            sqlalchemy.Column("_str", sqlalchemy.TEXT),
        )
        sa_engine = sqlalchemy.create_engine("sqlite:///%s" % DB_FILE)
        sa_metadata.create_all(sa_engine)
        
        print("\nSelect row NON pickle type")
        
        # sqlite4dummy, 0.0040
        st = time.clock()
        for row in self.engine.select_row(Select(self.non_pk.all)):
            pass
        print("sqlite4dummy elapse %.6f seconds." % (time.clock() - st))
        print("%s item returns." % 
              len(list(self.engine.execute("SELECT * FROM non_pk"))))
        
        # sqlalchemy, 0.0055
        st = time.clock()
        for row in sa_engine.execute(sqlalchemy.sql.select([sa_table])):
            pass
        print("sqlalchemy elapse %.6f seconds." % (time.clock() - st))
        print("%s item returns." %
              len(list(self.engine.execute("SELECT * FROM non_pk"))))
        
if __name__ == "__main__":
    unittest.main()