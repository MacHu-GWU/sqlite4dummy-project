#!/usr/bin/env python
# -*- coding: utf-8 -*-

from collections import OrderedDict, namedtuple
from sqlite4dummy.dtype import dtype
from sqlite4dummy.row import Row
from sqlite4dummy.sql import _SQL_Param, and_, or_, asc, desc
import sqlite4dummy.statement as statement
import sqlite3

###############################################################################
#                               Insert Class                                  #
###############################################################################

class Insert():
    """
    
    **中文文档**
    """
    def __init__(self, table):
        self.table = table
            
    def sql_from_record(self):
        """Generate the 'INSERT INTO table...' sqlite command for recrod
        insertion.
        
        Example::
    
            INSERT INTO table_name VALUES (?,?,...,?);
        
        **中文文档**
        
        生成INSERT INTO table ... Sqlite语句。
        """
        sql_INSERT_INTO = "INSERT INTO %s" % self.table.table_name
        sql_KEYWORD_VALUES = "VALUES"
        sql_QUESTION_MARK = "(%s)" % ", ".join(["?"] * len(self.table.all) )
        template = "%s\n%s\n\t%s;"
        self.sql = template % (sql_INSERT_INTO,
                               sql_KEYWORD_VALUES,
                               sql_QUESTION_MARK,)
        
    def sql_from_row(self, row):
        """Generate the 'INSERT INTO table...' sqlite command for row
        insertion.
        
        Example::
        
            INSERT INTO table_name 
                (column1, column2, ..., columnN) 
            VALUES 
                (?,?,...,?);
                
        **中文文档**
        
        生成INSERT INTO table ... Sqlite语句。
        """
        sql_INSERT_INTO = "INSERT INTO %s" % self.table.table_name
        sql_COLUMNS = "(%s)" % ", ".join(row.columns)
        sql_KEYWORD_VALUES = "VALUES"
        sql_QUESTION_MARK = "(%s)" % ", ".join(["?"] * len(row.columns) )
        template = "%s\n\t%s\n%s\n\t%s;"
        self.sql = template % (sql_INSERT_INTO,
                               sql_COLUMNS,
                               sql_KEYWORD_VALUES,
                               sql_QUESTION_MARK,)
        
###############################################################################
#                               Select Class                                  #
###############################################################################

class SelectObjectError(Exception):
    pass
        
class Select():
    """
    [CN]Select对象用于创建SQL select语句。 在我们执行Sqlite3Engine.select(Select(table.all))时,
    首先会调用Select.toSQL()方法创建SQL语句, 然后执行cursor。
    """
    def __init__(self, columns):
        """To create a Select object, you have to name a list of Column object has to select. And
        use where(), limit(), offset(), distinct(), order_by() method to specify your selection.
        """
        self.columns = columns
        
        self.selected_item = list()
        self._temp_columns = list() 
        for i in self.columns:
            if isinstance(i, Column):
                self.selected_item.append(i.full_name)
                self._temp_columns.append(Column(
                    i.column_name,
                    i.data_type,
                    ))
            else:
                try:
                    self.selected_item.append(i.param)
                    self._temp_columns.append(Column(
                        i.func_name,
                        dtype.REAL,
                        ))
                except:
                    raise SelectObjectError("Initiation Error")
        self._temp_table = Table("_temp_table", MetaData(), *self._temp_columns)
        # construct SELECT WHAT clause
        self.SELECT_WHAT_clause = "SELECT\t%s" % ",\n\t".\
            join(self.selected_item)
        
        # construct SELECT WHERE clause
        for i in self.columns:
            try:
                self.SELECT_FROM_clause = "FROM\t%s" % i.table_name
            except:
                pass
        
        self.WHERE_clause = None
        self.ORDER_BY_clause = None
        self.LIMIT_clause = None
        self.OFFSET_clause = None

    def where(self, *argv):
        """where() method is used to filter records. It takes arbitrary many 
        comparison of column and value. _SQL_Param object is created for 
        each comparison. And finally we produce the WHERE clause SQL.
        
        Example::
        
            >>> from sqlite4dummy
            >>> s = S
            where(column1 >= 3.14, column2.between(1, 100), column3.like("%pattern%"))
        """
        self.WHERE_clause = "WHERE\t%s" % "\n\tAND ".join([i.param for i in argv])
        return self
    
    def order_by(self, *argv):
        """Sort the result-set by one or more columns. you can custom the 
        priority of the orders and choose ascending or descending.
        
        valid argument:
            "column_name", defualt ascending
            asc("column_name"),
            desc("column_name"),
        
        example:
            order_by("column_name1", desc("column_name2"))
        
        function can automatically produce _Select_conifg() object by your setting
        """
        priority = list()
        for i in argv:
            if isinstance(i, Column):
                priority.append(asc(i).param)
            elif isinstance(i, str):
                priority.append("%s ASC" % i)
            else:
                priority.append(i.param)
        self.ORDER_BY_clause = "ORDER BY %s" % ", ".join(priority)
        return self
    
    def limit(self, howmany):
        """limit clause
        """
        self.LIMIT_clause = "LIMIT %s" % howmany
        return self
    
    def offset(self, howmany):
        """offset clause
        """
        self.OFFSET_clause = "OFFSET %s" % howmany
        return self
    
    def distinct(self):
        """distinct clause
        """
        self.SELECT_WHAT_clause = self.SELECT_WHAT_clause.replace(
            "SELECT\t", "SELECT DISTINCT\t")
        self.SELECT_WHAT_clause = self.SELECT_WHAT_clause.replace(
            "\n\t", "\n\t\t")
        return self
    
    def select_from(self, select_obj):
        self.SELECT_FROM_clause = "FROM\t(%s)" % select_obj.sql.replace(
                                                    "\n", "\n\t")
        return self
    
    @property
    def sql(self):
        return "\n".join([i for i in [
            self.SELECT_WHAT_clause,
            self.SELECT_FROM_clause,
            self.WHERE_clause,
            self.ORDER_BY_clause,
            self.LIMIT_clause,
            self.OFFSET_clause] if i ])
        
###############################################################################
#                               Column Class                                  #
###############################################################################

class Column():
    def __init__(self, column_name, data_type, 
                 nullable=True, default=None, primary_key=False):
        """
        """
        self.column_name = column_name
        self.data_type = data_type
        self.nullable = nullable
        self.default = default
        self.primary_key = primary_key
        
        self.to_sql_param = self.data_type.to_sql_param
        self.from_sql_param = self.data_type.from_sql_param
        self.is_pickletype = self.data_type.name == "PICKLETYPE"
        
    def __str__(self):
        """Return column name.
        """
        return self.column_name
    
    def __repr__(self):
        """Return the string represent the Column object, which can recover 
        the Column object from.
        """
        template = "Column('%s', dtype.%s, nullable=%s, default=%s, primary_key=%s)"
        return template % (
                    self.column_name,
                    self.data_type.name,
                    self.nullable,
                    repr(self.default),
                    self.primary_key,
                    )
    
    def bind_table(self, table):
        if isinstance(table, Table):
            self.table_name = table.table_name
            self.full_name = "%s.%s" % (table.table_name, self.column_name)
        else:
            raise TypeError()
    
    # comparison operator
    def __lt__(self, other):
        if isinstance(other, Column):
            return _SQL_Param("%s < %s" % (
                self.full_name, other.full_name)) 
        else:
            return _SQL_Param("%s < %s" % (
                self.full_name, self.to_sql_param(other)))

    def __le__(self, other):
        if isinstance(other, Column):
            return _SQL_Param("%s <= %s" % (
                self.full_name, other.full_name)) 
        else:
            return _SQL_Param("%s <= %s" % (
                self.full_name, self.to_sql_param(other)))
    
    def __eq__(self, other):
        if isinstance(other, Column):
            return _SQL_Param("%s = %s" % (
                self.full_name, other.full_name))
        else:
            if other == None: # if Column == None, means column_name is Null
                return _SQL_Param("%s IS NULL" % self.full_name)
            else:
                return _SQL_Param("%s = %s" % (
                    self.full_name, self.to_sql_param(other)))
        
    def __ne__(self, other):
        if isinstance(other, Column):
            return _SQL_Param("%s != %s" % (
                self.full_name, other.full_name))
        else:
            if other == None: # if Column != None, means column_name NOT Null
                return _SQL_Param("%s NOT NULL" % self.full_name)
            else:
                return _SQL_Param("%s != %s" % (
                    self.full_name, self.to_sql_param(other)))
        
    def __gt__(self, other):
        if isinstance(other, Column):
            return _SQL_Param("%s > %s" % (
                self.full_name, other.full_name))
        else:
            return _SQL_Param("%s > %s" % (
                self.full_name, self.to_sql_param(other)))
    
    def __ge__(self, other):
        if isinstance(other, Column):
            return _SQL_Param("%s >= %s" % (
                self.full_name, other.full_name))
        else:
            return _SQL_Param("%s >= %s" % (
                self.full_name, self.to_sql_param(other)))
    
    def between(self, lowerbound, upperbound):
        """WHERE...BETWEEN...AND... clause
        """
        if isinstance(lowerbound, Column) and isinstance(upperbound, Column):
            return _SQL_Param("%s BETWEEN %s AND %s" % (
                self.full_name,
                lowerbound.full_name,
                upperbound.full_name,
                ))
        elif isinstance(lowerbound, Column) and \
            (not isinstance(upperbound, Column)):
            return _SQL_Param("%s BETWEEN %s AND %s" % (
                self.full_name,
                lowerbound.full_name,
                self.to_sql_param(upperbound),
                ))
        elif isinstance(upperbound, Column) and \
            (not isinstance(lowerbound, Column)):
            return _SQL_Param("%s BETWEEN %s AND %s" % (
                self.full_name,
                self.to_sql_param(lowerbound),
                upperbound.full_name,
                ))
        else:
            return _SQL_Param("%s BETWEEN %s AND %s" % (
                self.full_name,
                self.to_sql_param(lowerbound),
                self.to_sql_param(upperbound),
                ))

    def like(self, wildcards):
        """WHERE...LIKE... clause
        """
        return _SQL_Param("%s LIKE %s" % (self.full_name, 
            self.to_sql_param(wildcards)))

    def in_(self, candidates):
        """WHERE...IN... clause
        """
        return _SQL_Param("%s IN (%s)" % (self.full_name,
            ", ".join([self.to_sql_param(candidate) for candidate in candidates])
            ))
        
###############################################################################
#                                Table Class                                  #
###############################################################################

class DuplicateColumnError(Exception):
    pass

class ColumnCollection():
    def __init__(self, *args):
        for column in args:
            if not isinstance(column, Column):
                raise TypeError("ColumnCollection has to be constructed "
                                "by couple of Column objects.")
            else:
                object.__setattr__(self, column.column_name, column)
        if len({column.column_name for column in args}) != len(args):
            raise DuplicateColumnError("Duplicate column name found!")

class Table():
    """Represent a table in a database.

    Define a Table::
        
        metadata 
        mytable = Table("mytable", metadata,
                    Column("mytable_id", INTEGER(), primary_key=True),
                    Column("value", TEXT()),
                    )

    columns can be accessed by table.c.column_name

    e.g.::
        mytable_id = mytable.c.mytable_id

    """

    def __init__(self, table_name, metadata, *args):
        self.table_name = table_name
        self.c = ColumnCollection(*args)
        self.all = list()
        self.column_names = list()
        
        self.primary_key_columns = list()
        self.pickletype_columns = list()
        
        for column in args:
            column.bind_table(self) # 将column与Table绑定
            self.all.append(column)
            self.column_names.append(column.column_name)
            if column.primary_key: # 定位PRIMARY KEY的列
                self.primary_key_columns.append(column.column_name)
            if column.is_pickletype: # 定位PICKLETYPE的列
                self.pickletype_columns.append(column.column_name)
                
        metadata._add_table(self)
        
    def __str__(self):
        """Return table name.
        """
        return self.table_name
    
    def __repr__(self):
        """Return the string represent the Table object, which can recover 
        the Table object from.
        """
        template = "Table('%s', MetaData(), \n\t%s\n\t)"
        return template % (
            self.table_name, 
            ",\n\t".join([repr(column) for column in self.all])
            )
        
    def __iter__(self):
        return iter(self.all)
    
    def get_column(self, column_name):
        return getattr(self.c, column_name)
    
    @property
    def create_table_sql(self):
        constructor = statement.CreateTable(self)
        return constructor.sql
    
    def insert(self):
        return Insert(self)
###############################################################################
#                               MetaData Class                                #
###############################################################################

class DuplicateTableError(Exception):
    pass

class MetaData():
    """A schema information container holds all Table objects in a database and
    their columns' schema definition and constructs.
    
    MetaData are also able to bind to an Sqlite3Engine object. If bound, more
    Table related SQL execution can be easily generated.
    
    Access all table::
    
        MetaData.tables
        
    Access table by table name::
    
        MetaData.get_table(table_name)
        
    Create all table::
    
        MetaData.create_all(engine)
    """
    def __init__(self, bind=None):
        self.t = dict()
        self.tables = list()
        if bind:
            if isinstance(bind, Sqlite3Engine):
                self.reflect(bind)
            else:
                raise Exception("You cannot bind metadata to '%s'!" % repr(bind))
        else:
            self.bind = bind
    
    def __str__(self):
        if self.bind:
            text_engine = "Binded with %s" % self.bind.dbname
        else:
            text_engine = "Binded with Nothing."
        text_table = "\n".join([repr(table) for table in self.tables])
        return "\n".join([text_engine, text_table])
    
    def _add_table(self, table):
        if table.table_name not in self.t:
            self.t[table.table_name] = table
            self.tables.append(table)
        else:
            raise DuplicateTableError("Duplicate table name found!")
        
    def _remove_table(self, table_name):
        table = self.t.pop(table_name)
        self.tables.remove(table)
    
    def __iter__(self):
        return iter(self.tables)
    
    def get_table(self, table_name):
        """Access table by table name.
        """
        return self.t[table_name]
    
    def create_all(self, engine):
        """Create all table in metadata in database engine. Also bind it self
        to this engine.
        """
        self.bind = engine
        for table in self.tables:
            create_table_sql = table.create_table_sql
            try:
                engine.execute(create_table_sql)
            except Exception as e:
                print(e)

    def reflect(self, engine, pickletype_columns=list()):
        """Read table, column metadata from database schema information.
        """
        self.bind = engine
        
        table_name_list = [record[0] for record in engine.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table';")]

        for table_name in table_name_list:
            columns = list()
            for record in engine.execute("PRAGMA table_info(%s)" % table_name):
                (_, column_name, column_type_name, 
                 not_null, default_value, is_primarykey) = record
                # 获得列的全名
                column_fullname = "%s.%s" % (table_name, column_name)
                # 获得列的数据类型的类实例
                column_type = dtype.get_dtype_by_name(column_type_name)
                # 根据特殊规则, 找到pickle type的列
                if (column_fullname in pickletype_columns) and \
                    (column_type.name == "BLOB"):
                    column_type = dtype.PICKLETYPE
                # 获得是否nullable
                nullable = not not_null
                # 获得default value in Python type
                default = column_type.from_sql_param(default_value)
                # 获得是否是primary_key
                primary_key = is_primarykey == 1
                # construct Column
                column = Column(column_name, column_type, 
                                primary_key=primary_key, nullable=nullable,
                                default=default)
                columns.append(column)
            
            table = Table(table_name, self, *columns)
    
if __name__ == "__main__":
    from sqlite4dummy.engine import Sqlite3Engine
    from datetime import datetime, date
    from pprint import pprint as ppt
    import unittest
    
    class ColumnUnittest(unittest.TestCase):
        def test_str_and_repr(self):
            column = Column("_id", dtype.INTEGER,
                            nullable=False, default=None, primary_key=True)
            self.assertEqual(str(column), "_id")
            self.assertEqual(repr(column),
                "Column('_id', dtype.INTEGER, nullable=False, default=None, primary_key=True)")
            
            column = Column("_text", dtype.TEXT, default="hello world")
            self.assertEqual(str(column), "_text")
            self.assertEqual(repr(column), 
                "Column('_text', dtype.TEXT, nullable=True, default='hello world', primary_key=False)")
            
            column = Column("_float", dtype.REAL, nullable=False)
            self.assertEqual(str(column), "_float")
            self.assertEqual(repr(column), 
                "Column('_float', dtype.REAL, nullable=False, default=None, primary_key=False)")

            column = Column("_byte", dtype.BLOB, default=b"8e01ad49")
            self.assertEqual(str(column), "_byte")
            self.assertEqual(repr(column), 
                "Column('_byte', dtype.BLOB, nullable=True, default=b'8e01ad49', primary_key=False)")
            
            column = Column("_date", dtype.DATE, default=date(2000, 1, 1))
            self.assertEqual(str(column), "_date")
            self.assertEqual(repr(column), 
                "Column('_date', dtype.DATE, nullable=True, default=datetime.date(2000, 1, 1), primary_key=False)")

            column = Column("_datetime", dtype.DATE, default=datetime(2015, 7, 15, 6, 30))
            self.assertEqual(str(column), "_datetime")
            self.assertEqual(repr(column), 
                "Column('_datetime', dtype.DATE, nullable=True, default=datetime.datetime(2015, 7, 15, 6, 30), primary_key=False)")

            column = Column("_pickle", dtype.PICKLETYPE, default=[1, 2, 3])
            self.assertEqual(str(column), "_pickle")
            self.assertEqual(repr(column), 
                "Column('_pickle', dtype.PICKLETYPE, nullable=True, default=[1, 2, 3], primary_key=False)")
            
        def test_comparison_operator(self):
            c = Column("other_column", dtype.TEXT)
            
            # integer type
            column = Column("_int", dtype.INTEGER)
            t = Table("test", MetaData(), c, column)
            
            self.assertEqual((column > 1).param, "test._int > 1")
            self.assertEqual((column >= 1).param, "test._int >= 1")
            self.assertEqual((column < 1).param, "test._int < 1")
            self.assertEqual((column <= 1).param, "test._int <= 1")
            self.assertEqual((column == 1).param, "test._int = 1")
            self.assertEqual((column != 1).param, "test._int != 1")
            self.assertEqual((column.between(0, 1)).param, 
                             "test._int BETWEEN 0 AND 1")
            self.assertEqual((column.in_([1, 2, 3])).param, "test._int IN (1, 2, 3)")
            
            # text type
            column = Column("_text", dtype.TEXT)
            t = Table("test", MetaData(), c, column)
            
            self.assertEqual((column > "abc").param,
                             "test._text > 'abc'")
            self.assertEqual((column >= "abc").param,
                             "test._text >= 'abc'")
            self.assertEqual((column < "abc").param,
                             "test._text < 'abc'")
            self.assertEqual((column <= "abc").param,
                             "test._text <= 'abc'")
            self.assertEqual((column == "abc").param,
                             "test._text = 'abc'")
            self.assertEqual((column != "abc").param,
                             "test._text != 'abc'")
            self.assertEqual((column.like("%abc%")).param,
                             "test._text LIKE '%abc%'")
            
            # date type
            column = Column("_date", dtype.DATE)
            t = Table("test", MetaData(), c, column)
            
            self.assertEqual((column > date(2000, 1, 1)).param, 
                             "test._date > '2000-01-01'")
            self.assertEqual((column >= date(2000, 1, 1)).param, 
                             "test._date >= '2000-01-01'")
            self.assertEqual((column < date(2000, 1, 1)).param, 
                             "test._date < '2000-01-01'")
            self.assertEqual((column <= date(2000, 1, 1)).param, 
                             "test._date <= '2000-01-01'")
            self.assertEqual((column == date(2000, 1, 1)).param, 
                             "test._date = '2000-01-01'")
            self.assertEqual((column != date(2000, 1, 1)).param, 
                             "test._date != '2000-01-01'")
            self.assertEqual((column.between(date(2000, 1, 1), 
                                             date(2000, 12, 31))).param, 
                             "test._date BETWEEN '2000-01-01' AND '2000-12-31'")
            
            # blob type
            column = Column("_blob", dtype.BLOB)
            t = Table("test", MetaData(), c, column)
            
            self.assertEqual((column == b"8e01ad49").param, 
                             "test._blob = X'3865303161643439'")
            self.assertEqual((column != b"8e01ad49").param, 
                             "test._blob != X'3865303161643439'")
            
            # pickle type
            column = Column("_pickle", dtype.PICKLETYPE)
            t = Table("test", MetaData(), c, column)
            
            self.assertEqual((column == [1, 2, 3]).param, 
                             "test._pickle = X'80035d7100284b014b024b03652e'")
            self.assertEqual((column != [1, 2, 3]).param, 
                             "test._pickle != X'80035d7100284b014b024b03652e'")
            
            # None type
            self.assertEqual((column == None).param,
                             "test._pickle IS NULL")
            self.assertEqual((column != None).param,
                             "test._pickle NOT NULL")
            
            # column type, 列与列比较
            column = Column("_this", dtype.TEXT)
            t = Table("test", MetaData(), c, column)
            
            self.assertEqual((column > c).param, "test._this > test.other_column")
            self.assertEqual((column >= c).param, "test._this >= test.other_column")
            self.assertEqual((column < c).param, "test._this < test.other_column")
            self.assertEqual((column <= c).param, "test._this <= test.other_column")
            self.assertEqual((column == c).param, "test._this = test.other_column")
            self.assertEqual((column != c).param, "test._this != test.other_column")
            self.assertEqual((column.between(c, c)).param, 
                             "test._this BETWEEN test.other_column AND test.other_column")
            self.assertEqual((column.between("abc", c)).param, 
                             "test._this BETWEEN 'abc' AND test.other_column")
            self.assertEqual((column.between(c, "abc")).param, 
                             "test._this BETWEEN test.other_column AND 'abc'")
            self.assertEqual((column.between("abc", "xyz")).param, 
                             "test._this BETWEEN 'abc' AND 'xyz'")
            
    class TableUnittest(unittest.TestCase):
        def setUp(self):
            self.table = Table("employee", MetaData(),
                Column("_id", dtype.TEXT, primary_key=True),
                Column("name", dtype.TEXT, nullable=False),
                Column("date_of_birth", dtype.DATE, nullable=False),
                Column("height", dtype.REAL),
                Column("profile", dtype.PICKLETYPE, default={
                                        "role": list(), 
                                        "department": None,
                                        }),
                Column("memo", dtype.TEXT, default="This guy is lazy, no memo."),
                )
        
        def test_str(self):
            self.assertEqual(self.table.table_name, "employee")
            
        def test_repr(self):
#             print(repr(self.table))
            pass
        
        def test_iter(self):
            """测试对Table的for循环
            """
            for column in self.table:
                self.assertEqual(column.column_name, 
                     self.table.get_column(column.column_name).column_name)
        
        def test_get_column(self):
            self.assertEqual(self.table.get_column("_id").column_name, "_id")
            self.assertRaises(AttributeError, self.table.get_column, "NOTHING")
            
            
        def test_bind_column(self):
            """测试初始化Table之后是否会将Column.table_name, Column.full_name
            绑定上。
            """
            for column in self.table:
                self.assertEqual(column.table_name, self.table.table_name)
                self.assertEqual(column.full_name,
                                 "%s.%s" % (self.table, column))
            
            for column in self.table.all:
                self.assertEqual(column.full_name,
                                 "%s.%s" % (self.table, column))

    
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