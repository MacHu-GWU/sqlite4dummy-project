##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    
    
Module description
------------------
    SQLITE is a Python sqlite3 SQL toolkit that gives application developers the full power and
    flexibility of SQL. Sounds like SqlAlchemy? Right! But in sqlite3, it's even faster than
    SqlAlchemy. And SQLITE is easier to be extend with User Customized DataType, transaction flow.
    Anyway, the original idea is from SqlAlchemy. Thanks for SqlAlchemy.
    
    Why it's faster than SqlAlchemy in sqlite?
        because sqlite is single writer database designed for simple but high volumn I/O task.
        so SqlAlchemy is like 3-5 slower than the original python sqlite3 API.
    
    
Keyword
-------
    sqlite3, database
    
    
Compatibility
-------------
    Python2: No
    Python3: Yes


Prerequisites
-------------
    None


Import Command
--------------
    from .core import (MetaData, Sqlite3Engine, Table, Column, DataType, Row, 
        and_, or_, desc, Select)
"""

from angora.DATA.dtype import StrSet, IntSet, StrList, IntList
from collections import OrderedDict
import sqlite3
import pickle

def obj2bytestr(obj):
    """convert arbitrary object to database friendly bytestr"""
    return pickle.dumps(obj)

def bytestr2obj(bytestr):
    """recovery object from bytestr"""
    return pickle.loads(bytestr)

def bytestr2hexstring(bytestr):
    """convert byte string to hex string, for example
    b'\x80\x03]q\x00(K\x01K\x02K\x03e.'  to  X'80035d7100284b014b024b03652e'
    """
    res = list()
    for i in bytestr:
        res.append(str(hex(i))[2:].zfill(2))
    return "".join(res)

def hexstring2bytestr(hexstring):
    """convert hex string to byte string, for example
    X'80035d7100284b014b024b03652e' to b'\x80\x03]q\x00(K\x01K\x02K\x03e.'
    """
    res = list()
    temp = list()
    for i in hexstring:
        temp.append(i)
        if len(temp) == 2: # 每两个字符为一位, 将其按照16进制解码成整数, 转化成bytes
            res.append(bytes([int("".join(temp), 16)]))
            temp = list()
    return b"".join(res) # 合并成一个bytestr

##################################################
#                                                #
#                   Row class                    #
#                                                #
##################################################

class Row():
    """
    [CN]数据表中的行数据类。 可以使用索引Row[column_name]或是属性Row.column_name的方式对数据进行访问。
    [EN]An abstract class for row object in database table. Values can be visit by it's column name
    in two way: Row[column_name], Row.column_name
    """
    def __init__(self, columns, values):
        self.columns = columns
        self.values = values
        self.dictionary_view = None
    
    @staticmethod
    def from_dict(dictionary):
        """
        [EN]Generate a Row object from a python dictionary
        [CN]从一个字典中生成Row对象
        """
        return Row(tuple(dictionary.keys()), tuple(dictionary.values()))
    
    def to_dict(self):
        self._smart_create_dict_view()
        return self.dictionary_view
    
    def _create_dict_view(self):
        """
        [CN]为了节约内存, 在初始化Row对象时仅仅是以两个列表的形式储存 column : value 信息。但是在
        Row[column_name], Row.column_name, Row[column_name]=value, print(Row)的时候会自动从
        columns, values中生成一个字典。这样在仅仅用于Insert的时候, 不需要创建字典, 可以直接传入
        行tuple, 从而节约了计算量。
        """
        self.dictionary_view = OrderedDict()
        for column_name, value in zip(self.columns, self.values):
            self.dictionary_view[column_name] = value
    
    def _smart_create_dict_view(self):
        """在_create_dict_view之前进行判断是否需要重新创建dictionary_view
        """
        if not self.dictionary_view:
            self._create_dict_view()
            
    def __str__(self):
        self._smart_create_dict_view()
        return str(self.dictionary_view)
    
    def __repr__(self):
        return "Row(columns=%s, values=%s)" % (self.columns, self.values)
    
    def __getitem__(self, key):
        self._smart_create_dict_view()
        return self.dictionary_view[key]
    
    def __setitem__(self, key, value):
        self._smart_create_dict_view()
        if key in self.dictionary_view:
            self.dictionary_view[key] = value
            self.columns = tuple(self.dictionary_view.keys())
            self.values = tuple(self.dictionary_view.values())
        else:
            raise KeyError
        
    def __getattr__(self, attr):
        self._smart_create_dict_view()
        return self.dictionary_view[attr]
    
    def __eq__(self, other):
        self._smart_create_dict_view()
        other._smart_create_dict_view()
        return dict(self.dictionary_view) == dict(other.dictionary_view)
    
    
##################################################
#                                                #
#                 Insert class                   #
#                                                #
##################################################

class Insert():
    """
    [CN]Insert对象可以通过Table.insert()命令生成。当我们执行:
        Sqlite3Engine.insert_record时, 会执行Insert.sqlcmd_from_record以生成INSERT SQL命令
        最后再执行cursor.execute(Insert.sqlcmd, Insert.default_record_converter(record))完成插入

        Sqlite3Engine.insert_row时, 会执行Insert.sqlcmd_from_row(Row)以生成INSERT SQL命令
        最后再执行cursor.execute(Insert.sqlcmd, Insert.default_row_converter(Row))完成插入
    """
    def __init__(self, table):
        self.table = table
        if len(table.pickletype_columns) == 0: # Define default record/row converter
            self.default_record_converter = self.nonpicklize_record
            self.default_row_converter = self.nonpicklize_row
        else:
            self.default_record_converter = self.picklize_record
            self.default_row_converter = self.picklize_row
            
    def sqlcmd_from_record(self):
        """generate the 'INSERT INTO table...' SQL command suit for record, for example:
        INSERT INTO table_name VALUES (?,?,...,?);
        """
        cmd_INSERT_INTO = "INSERT INTO %s" % self.table.table_name
        cmd_COLUMNS = "(%s)" % ", ".join(self.table.columns)
        cmd_KEYWORD_VALUES = "VALUES"
        cmd_QUESTION_MARK = "(%s)" % ", ".join(["?"] * len(self.table.columns) )
        template = "%s\n\t%s\n%s\n\t%s;"
        self.insert_sqlcmd = template % (cmd_INSERT_INTO,
                                         cmd_COLUMNS,
                                         cmd_KEYWORD_VALUES,
                                         cmd_QUESTION_MARK,)
        
    def sqlcmd_from_row(self, row):
        """generate the 'INSERT INTO table...' SQL command suit for row, for example:
        INSERT INTO table_name (column1, column2, ..., columnN) VALUES (?,?,...,?);
        """
        cmd_INSERT_INTO = "INSERT INTO %s" % self.table.table_name
        cmd_COLUMNS = "(%s)" % ", ".join(row.columns)
        cmd_KEYWORD_VALUES = "VALUES"
        cmd_QUESTION_MARK = "(%s)" % ", ".join(["?"] * len(row.columns) )
        template = "%s\n\t%s\n%s\n\t%s;"
        self.insert_sqlcmd = template % (cmd_INSERT_INTO,
                                         cmd_COLUMNS,
                                         cmd_KEYWORD_VALUES,
                                         cmd_QUESTION_MARK,)
    
    ### record/row converter to change the record/row to sqlite3 friendly tuple
    def nonpicklize_record(self, record):
        """把一个不含PICKLETYPE的record原样返回
        """
        return record
            
    def picklize_record(self, record):
        """把一个含有PICKLETYPE的record中的pickle对应列转化成pickle string再返回
        """
        new_record = list()
        for column, item in zip(self.table.columns.values(), record):
            if column.is_pickletype:
                if item: # 如果 item 不为 None, 则需要处理成bytestr
                    new_record.append(obj2bytestr(item))
                else: # 如果 item 为 None, 则不处理
                    new_record.append(item)
            else:
                new_record.append(item)
        return tuple(new_record)

    def nonpicklize_row(self, row):
        """把一个不含PICKLETYPE的row其中的values, 即行的tuple原样返回
        """
        return row.values

    def picklize_row(self, row):
        """把一个含有PICKLETYPE的row中的pickle对应列转化成pickle string再封装成tuple返回
        """
        new_record = list()
        for column, item in zip(row.columns, row.values):
            if self.table.columns[column].is_pickletype:
                if item: # 如果 item 不为 None, 则需要处理成bytestr
                    new_record.append(obj2bytestr(item))
                else: # 如果 item 为 None, 则不处理
                    new_record.append(item)
            else:
                new_record.append(item)
        return tuple(new_record)


##################################################
#                                                #
#                 Update class                   #
#                                                #
##################################################

class _Update_config():
    """用于判断每个列上的值是绝对更新还是相对更新
    由于UPDATE语法中相对更新的情况下会出现下面的情况:
        UPDATE table_name SET column_name1 = column_name2 + 1 WHERE...
    所以对于Column对象, 我们定义了 __add__ 等运算符方法, 将 Column + 数值 定义为返回一个_Update_config
    对象。这样在 Update.values(**kwarg) 方法中我们就可以将 Column + 数值 作文sql字符串进行输入了
    
    目前只支持 column 和 数值进行运算, 并且只能有两项。
    """
    def __init__(self, sqlcmd):
        self.sqlcmd = sqlcmd

class Update():
    """
    [CN]Update对象可以通过Table.update()命令生成。当我们执行:
        Sqlite3Engine.update(update_obj)时, 会执行Update.sqlcmd()方法以更新Update.update_sqlcmd,
        然后执行cursor.execute(Update.update_sqlcmd)以完成更新
    """
    def __init__(self, table):
        self.table = table
        self.update_clause = "UPDATE %s" % self.table.table_name
        self.where_clause = None
        
    def values(self, **kwarg):
        """
        SET
            movie.length = movie.length + 9999, # 相对更新
            movie.title ="ABCD", # 绝对更新
            movie.genres = 'gANjYnVpbHRpbnMKc2V0CnEAXXEBWAkAAABBZHZlbnR1cmVxAmGFcQNScQQu', # 绝对更新
            movie.release_date = '1500-01-01' # 绝对更新
        """
        res = list()
        for column_name, value in kwarg.items():
            if value == None: # 有时候我们要把值更新为Null, 这样我们在values中设定=None即可
                res.append("%s = %s" % (column_name, "NULL"))
            else:
                column = self.table.columns[column_name]
                try: # value是_Update_config对象, 处理相对更新
                    res.append("%s = %s" % (column.column_name, 
                                            value.sqlcmd ) ) # 直接使用
                except: # value是一个值, 处理绝对更新
                    if isinstance(value, str):
                        res.append("%s = '%s'" % (column.column_name, 
                                                value.replace("'", "''").replace('"', '\"') ) ) # 将 = value 中 value 的部分处理
                        value = value.replace("'", "''").replace('"', '\"')
                    else:
                        res.append("%s = %s" % (column.column_name, 
                                                column.__SQL__(value) ) ) # 将 = value 中 value 的部分处理
            
        self.set_clause = "SET\n\t%s" % ",\n\t".join(res)
        return self
    
    def where(self, *argv):
        """define WHERE clause in UPDATE SQL command
        """
        self.where_clause = "WHERE\n\t%s" % " AND\n\t".join([_select_config.sqlcmd for _select_config in argv])
        return self
    
    def sqlcmd(self):
        """generate "UPDATE table SET..." SQL command
        
        example output:
        
            update = movie.update().values(
                movie.genres = {1,2,3}, 
                movie.length = movie.length + 9999,
                movie.release_date = '1500-01-01',
                movie.title = 'ABCDEFG').where(
                    movie.release_date <= '2000-01-01',
                    movie.length > 100)
            update.sqlcmd() # update.update_sqlcmd = the following
        
            UPDATE movie
            SET
                genres = 'gANjYnVpbHRpbnMKc2V0CnEAXXEBWAkAAABBZHZlbnR1cmVxAmGFcQNScQQu',
                length = length + 9999,
                release_date = '1500-01-01',
                title = 'ABCDEFG'
            WHERE
                release_date <= '2000-01-01' AND
                length > 100
        """
        self.update_sqlcmd = "\n".join([i for i in [self.update_clause, 
                                                   self.set_clause, 
                                                   self.where_clause] if i])


##################################################
#                                                #
#                 Select class                   #
#                                                #
##################################################

def and_(*argv):
    """AND join list of where clause criterions
    """
    return _Select_config("(%s)" % " AND ".join([i.sqlcmd for i in argv]))

def or_(*argv):
    """OR join list of where clause criterions
    """
    return _Select_config("(%s)" % " OR ".join([i.sqlcmd for i in argv]))

def asc(column_name):
    """sort results by column_name in ascending order
    """
    return _Select_config("%s ASC" % column_name)

def desc(column_name):
    """sort results by column_name in descending order
    """
    return _Select_config("%s DESC" % column_name)

class _Select_config():
    """_Select_config is a internal (for developer only) class to set up WHERE clause
    Usually it's generated by a comparison operator of a Column object and a value.
    For example:
        Column >= 100
        Column.between(100, 200)
        Column.like("pattern")
    """
    def __init__(self, sqlcmd):
        self.sqlcmd = sqlcmd

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
        self.column_names = tuple([column.column_name for column in self.columns])
        
        self.select_from_clause = "SELECT %s FROM %s" % (", ".join(self.column_names), 
                                                         self.columns[0].table_name)
        self.where_clause = None
        self.orderby_clause = None
        self.limit_clause = None
        self.offset_clause = None
        self.distinct_clause = None
        
        # Define default record converter, convert pickletype byte string back to python object
        if len([column for column in self.columns if column.is_pickletype]) == 0: 
            self.default_record_converter = self.nonpicklize_record
        else:
            self.default_record_converter = self.picklize_record

    def where(self, *argv):
        """where() method is used to filter records. It takes arbitrary many comparison of 
        column and a value as Input, and construct a _Select_config() object to store these
        information. And finally produce the where_clause SQL.
        
        example:
            where(column1 >= 3.14, column2.between(1, 100), column3.like("%pattern%"))
        """
        self.where_clause = "WHERE %s" % " AND ".join([i.sqlcmd for i in argv])
        return self
    
    def limit(self, howmany):
        """limit clause
        """
        self.limit_clause = "LIMIT %s" % howmany
        return self
    
    def offset(self, howmany):
        """offset clause
        """
        self.offset_clause = "OFFSET %s" % howmany
        return self
    
    def distinct(self):
        self.select_from_clause = self.select_from_clause.replace("SELECT", "SELECT DISTINCT")
        return self
    
    def order_by(self, *argv):
        """sort the result-set by one or more columns. you can name the order of the columns and
        ascending or descending.
        
        valid argument:
            "column_name", defualt ascending
            asc("column_name"),
            desc("column_name"),
        
        example:
            order_by("column_name1", desc("column_name2"))
        
        function can automatically produce _Select_conifg() object by your setting
        """
        new_argv = list()
        for i in argv:
            if isinstance(i, _Select_config):
                new_argv.append(i)
            else:
                new_argv.append(asc(i))
        self.orderby_clause = "ORDER BY %s" % ", ".join([i.sqlcmd for i in new_argv])
        return self
        
    def __str__(self):
        return "\n\t".join([i for i in [self.select_from_clause,
                                        self.where_clause,
                                        self.orderby_clause,
                                        self.limit_clause,
                                        self.offset_clause] if i ])
    
    def toSQL(self):
        """return the SELECT SQL command"""
        return str(self)

    ### record converter to change the record to sqlite3 friendly tuple
    def nonpicklize_record(self, record):
        """把一个不含PICKLETYPE的record原样返回
        """
        return record
            
    def picklize_record(self, record):
        """把一个含有PICKLETYPE的record中的pickle对应列转化成pickle string再返回
        """
        new_record = list()
        for column, item in zip(self.columns, record):
            if column.is_pickletype:
                if item: # 如果 item 不为 None, 则需要处理成bytestr
                    new_record.append(bytestr2obj(item))
                else: # 如果 item 为 None, 则不处理
                    new_record.append(item)
            else:
                new_record.append(item)
        return tuple(new_record)


##################################################
#                                                #
#                MetaData class                  #
#                                                #
##################################################

class MetaData():
    def __init__(self, bind=None):
        self.tables = dict()
        if bind:
            self.reflect(bind)
        else:
            self.bind = bind
    
    def __str__(self):
        return "\n".join([repr(table) for table in self.tables.values()])
    
    def get_table(self, table_name):
        return self.tables[table_name]
    
    def create_all(self, engine):
        """create all table stored in metadata through the engine
        """
        self.bind = engine
        for table in self.tables.values():
            create_table_sqlcmd = table.create_table_sql()
            try:
                engine.cursor.execute(create_table_sqlcmd)
            except Exception as e:
                pass
#                 print(e)
    
    def drop_all(self, engine):
        """drop all table stored in this metadata
        """
        for table in self.tables.values():
            table.drop(engine)
    
    ### ========== Reflect 方法所需的方法, 主要用来处理column中的default value ===========
    def _eval_converter(self, text):
        """
        """
        if text:
            return eval(text)
        else:
            return None
        
    def _blob_converter(self, text):
        """如果某一列是pickletype或是BlOB字节类型, 那么数据库中储存的值是:
        b'\x80\x03]q\x00(K\x01K\x02K\x03e.' type bytes
        但是从数据库中读取metadata的时候, 读出来的是其16进制并转化成了的字符串的形式, 形如
        "X'80035d7100284b014b024b03652e'" type str
        
        那么我们要从这个字符串中解析出原来的python对象, 我们就需要:
            1. 首先对字符串进行截取, 取str[2:-1], 获得数字的部分
            2. 然后再用hextstring2bytestr转化成bytestr, b'\x80\x03]q\x00(K\x01K\x02K\x03e.'
            3. 最后再用bytestr2obj恢复成对象
        """
        if text:
            return bytestr2obj(hexstring2bytestr(text[2:-1]))
        else:
            return None
        
    def _strset_converter(self, text):
        return StrSet.sqlite3_converter(eval(text))
    
    def _intset_converter(self, text):
        return IntSet.sqlite3_converter(eval(text))
    
    def _strlist_converter(self, text):
        return StrList.sqlite3_converter(eval(text))
    
    def _intlist_converter(self, text):
        return IntList.sqlite3_converter(eval(text))
    
    def reflect(self, engine):
        """read sqlite3 metadata, create the database schema.
        [CN], 我们需要
        讲其中的dtype

        dtype_mapping:
            执行engine.execute("PRAGMA table_info(table_name)")时会返回每列的column_dtype_name。
            我们要将column_dtype_name对应到DATATYPE对象。

        default_value_maping: 
            执行engine.execute("PRAGMA table_info(table_name)")时会返回每列的default_value。
            default_value是一个字符串。 例如:
                列的default="foobar", 那么sqlite3的特殊表中储存的就是"'foobar'", 会多出来两个字符串标志。
            所以我们要根据column_dtype_name使用不同的函数将default_value转换成原始的default, 也就是Python
            对象。 
        """
        self.bind = engine
        dtype_mapping = {
            "TEXT": TEXT(), "INTEGER": INTEGER(), "REAL": REAL(), "DATE": DATE(), 
            "TIMESTAMP": DATETIME(), "BLOB": PICKLETYPE(),
            "PYTHONLIST": PYTHONLIST(), "PYTHONSET": PYTHONSET(),
            "PYTHONDICT": PYTHONDICT(), "ORDEREDDICT": ORDEREDDICT(),
            "STRSET": STRSET(), "INTSET": INTSET(),
            "STRLIST": STRLIST(), "INTLIST": INTLIST(),
            }
        
        default_value_mapping = {
            "TEXT": self._eval_converter, "INTEGER": self._eval_converter, 
            "REAL": self._eval_converter, "DATE": self._eval_converter, 
            "TIMESTAMP": self._eval_converter, "BLOB": self._blob_converter,
            "PYTHONLIST": self._blob_converter, "PYTHONSET": self._blob_converter,
            "PYTHONDICT": self._blob_converter, "ORDEREDDICT": self._blob_converter,
            "STRSET": self._strset_converter, "INTSET": self._intset_converter,
            "STRLIST": self._strlist_converter, "INTLIST": self._intlist_converter,
            }
        
        table_name_list = list(engine.execute("""SELECT name FROM sqlite_master 
            WHERE type='table';"""))
        
        for table_name in table_name_list:
            table_name = table_name[0]
            columns = list()
            for record in engine.execute("PRAGMA table_info(%s)" % table_name):
                _, column_name, column_type_name, not_null, default_value, is_primarykey = record
                column = Column(column_name, dtype_mapping[column_type_name], 
                                primary_key=is_primarykey==1, nullable=not not_null,
                                default=default_value_mapping[column_type_name](default_value),)
                columns.append(column)
            table = Table(table_name, self, *columns)
            
##################################################
#                                                #
#                DataType class                  #
#                                                #
##################################################

class BaseDataType():
    """所有数据类型的父类
    """
    def __str__(self):
        return self.name
    
    def __repr__(self):
        return "%s()" % self.name

### Sqlite3内置类
### Sqlite3 built-in data type
class TEXT(BaseDataType):
    name = "TEXT"
    sqlite_dtype_name = "TEXT"

class INTEGER(BaseDataType):
    name = "INTEGER"
    sqlite_dtype_name = "INTEGER"
    
class REAL(BaseDataType):
    name = "REAL"
    sqlite_dtype_name = "REAL"
    
class DATE(BaseDataType):
    name = "DATE"
    sqlite_dtype_name = "DATE"
    
class DATETIME(BaseDataType):
    name = "DATETIME"
    sqlite_dtype_name = "TIMESTAMP"

### 用户自定义类
### user customized data type
# 什么时候用PICKLETYPE, 什么时候用PYTHONLIST, PYTHONSET, PYTHONDICT？
#     对于PICKLETYPE可以接受任意的Python对象, 若用原生的cursor.execute执行select语句时
#     返回的将是byte string而不是转换过后的对象本身。而只有用Select API进行select时候才
#     能返回正确的对象。因为考虑到sqlite3注册转换器时一对转换器只能接受一种type。所以不
#     可能用一对转换器处理任意Python对象。所以针对PICKLETYPE采用了特别的处理方式
#
#     而对于PYTHONLIST, PYTHONSET, PYTHONDICT其实同样是采用pickle.dumps, pickle.loads这样
#     的转换器。 但是由于注册了转换器, 所以用户既可以用Select API也可以用原生的cursor进行
#     选择, 都能得到正确的对象。
#
#     对于StrSet, IntSet, StrList, IntList这四种对象, 由于用字符串join的方式要比pickle.dumps
#     方式I/O的速度要快, 所以我们注册了特殊的转换器。
#
#     以上所有的特殊自定义类将会影响到Column.__SQL__()方法。这是由于在Sql语句中要将类转化为
#     合法的字符串。详情请参考Column.__SQL__()方法的部分。

class PICKLETYPE(BaseDataType):
    """
    [EN]Can be use to store any pickleable python type
    [CN]可以用来储存任意pickleable的python对象
    """
    name = "PICKLETYPE"
    sqlite_dtype_name = "BLOB"

class PYTHONLIST(BaseDataType):
    name = "PYTHONLIST"
    sqlite_dtype_name = "PYTHONLIST"
    
class PYTHONSET(BaseDataType):
    name = "PYTHONSET"
    sqlite_dtype_name = "PYTHONSET"
   
class PYTHONDICT(BaseDataType):
    name = "PYTHONDICT"
    sqlite_dtype_name = "PYTHONDICT"
    
class ORDEREDDICT(BaseDataType):
    name = "ORDEREDDICT"
    sqlite_dtype_name = "ORDEREDDICT"

class STRSET(BaseDataType):
    name = "STRSET"
    sqlite_dtype_name = "STRSET"
    
class INTSET(BaseDataType):
    name = "INTSET"
    sqlite_dtype_name = "INTSET"

class STRLIST(BaseDataType):
    name = "STRLIST"
    sqlite_dtype_name = "STRLIST"
    
class INTLIST(BaseDataType):
    name = "INTLIST"
    sqlite_dtype_name = "INTLIST"

class DataType():
    """数据类型的容器类, 用于通过DataType.text来调用TEXT(), 可以解决与其他库里面同样需要
    import TEXT, INTEGER, ... 然后发生命名空间冲突的问题。
    """
    text = TEXT()
    integer = INTEGER()
    real = REAL()
    date = DATE()
    datetime = DATETIME()
    pickletype = PICKLETYPE()
    
    pythonlist = PYTHONLIST()
    pythonset = PYTHONSET()
    pythondict = PYTHONDICT()
    ordereddict = ORDEREDDICT()
    strset = STRSET()
    intset = INTSET()
    strlist = STRLIST()
    intlist = INTLIST()
    
##################################################
#                                                #
#          Datatype Sqlite3 Converter            #
#                                                #
##################################################

def adapt_list(_LIST):
    """类 -> 字符串 转换"""
    return obj2bytestr(_LIST)

def convert_list(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

def adapt_set(_SET):
    """类 -> 字符串 转换"""
    return obj2bytestr(_SET)

def convert_set(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

def adapt_dict(_DICT):
    """类 -> 字符串 转换"""
    return obj2bytestr(_DICT)

def convert_dict(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

def adapt_ordereddict(_ORDEREDDICT):
    """类 -> 字符串 转换"""
    return obj2bytestr(_ORDEREDDICT)

def convert_ordereddict(_STRING):
    """字符串 -> 类 转换"""
    return bytestr2obj(_STRING)

##################################################
#                                                #
#                 Column class                   #
#                                                #
##################################################

class Column():
    """

    """
    def __init__(self, column_name, data_type, primary_key=False, nullable=True, default=None):
        if column_name in ["table_name", "columns", "primary_key_columns", "pickletype_columns", "all"]:
            raise Exception("""column name cannot use those system reserved name:
            "table_name", "columns", "primary_key_columns", "pickletype_columns", "all";""")
        
        self.column_name = column_name
        self.table_name = None
        self.full_name = None
        self.data_type = data_type
        self.is_pickletype = self.data_type.name == "PICKLETYPE"
        self.primary_key = primary_key
        self.nullable = nullable
        self.default = default
        
        # 在SQL语句中我们表示数字, 日期, 字符串, 对象都有不同的格式。例如:
        #     Default 'unknown'
        #     WHERE column_name >= '2014-01-01'
        #     SET column_name = 128
        # 所以我们需要一些特殊的方法处理每一种数据类型在Sql语句中的字符串
        
        __SQL__method_mapping = {
            "TEXT": self._sql_STRING_NUMBER,
            "INTEGER": self._sql_STRING_NUMBER,
            "REAL": self._sql_STRING_NUMBER,
            "DATE": self._sql_DATE,
            "DATETIME": self._sql_DATETIME,
            "PICKLETYPE": self._sql_PICKLETYPE,
            "PYTHONLIST": self._sql_PICKLETYPE,
            "PYTHONSET": self._sql_PICKLETYPE,
            "PYTHONDICT": self._sql_PICKLETYPE,
            "ORDEREDDICT": self._sql_PICKLETYPE,
            "STRSET": self._sql_STRSET,
            "INTSET": self._sql_INTSET,
            "STRLIST": self._sql_STRLIST,
            "INTLIST": self._sql_INTLIST,
            }
        self.__SQL__ = __SQL__method_mapping[self.data_type.name]
             
    def __str__(self):
        """return column_name
        """
        return self.column_name

    def __repr__(self):
        """return the string represent the Column object that can recover the object from it
        """
        return "Column('%s', %s, primary_key=%s, nullable=%s, default=%s)" % (self.column_name,
                                                                              repr(self.data_type),
                                                                              self.primary_key,
                                                                              self.nullable,
                                                                              repr(self.default),)

    # 下面这些_sql开头的方法是用于让不同的数据类型在SQL语句中正确的显示, 比如字符串的两边在SQL中
    # 要加'', 比如byte在Sql中是以 X'0482e0891ab87' 的形式表达的。
    # 所有的这些方法都跟Column所支持的数据类型在 __SQL__method_mapping 字典中做了一一对应

    def _sql_STRING_NUMBER(self, value):
        """if it is string or number, in sql command it's just 'string', number
        """
        return repr(value)

    def _sql_DATE(self, value):
        """if it is datetime.date object, in sql command it's just '1999-01-01'
        """
        return repr(str(value))
    
    def _sql_DATETIME(self, value):
        """if it is datetime.datetime object, in sql command it's just '1999-01-01 06:30:00'
        """
        return repr(str(value)[:19])

    def _sql_PICKLETYPE(self, value):
        """if it is python object, in sql command we convert it to byte string, like 'gx4=fjl82d...'
        """
        return "X'%s'" % bytestr2hexstring(obj2bytestr(value))

    def _sql_STRSET(self, value):
        """if it is StrSet, in sql commend we use 'item1&&item2&&...&&itemN'
        """
        return repr(StrSet.sqlite3_adaptor(value))
    
    def _sql_INTSET(self, value):
        """if it is IntSet, in sql commend we use 'item1&&item2&&...&&itemN'
        """
        return repr(IntSet.sqlite3_adaptor(value))
    
    def _sql_STRLIST(self, value):
        """if it is StrList, in sql commend we use 'item1&&item2&&...&&itemN'
        """
        return repr(StrList.sqlite3_adaptor(value))
    
    def _sql_INTLIST(self, value):
        """if it is IntList, in sql commend we use 'item1&&item2&&...&&itemN'
        """
        return repr(IntList.sqlite3_adaptor(value))

    def create_table_sql(self):
        """generate the definition part of 'CREATE TABLE (...)' SQL command
        by column name, data type, constrains.
        
        example output:
        
            movie_id TEXT
            title TEXT DEFAULT 'unknown_title'
            length INTEGER DEFAULT -1
            release_date DATE DEFAULT '0000-01-01'
            genres TEXT DEFAULT 'gANjYnVpbHRpbnMKc2V0CnEAXXEBhXECUnEDLg=='
        """
        name_part = self.column_name
        dtype_part = self.data_type.sqlite_dtype_name
        if self.nullable == True:
            nullable_part = None
        else:
            nullable_part = "NOT NULL"
        if self.default == None:
            default_part = None
        else:
            default_part = "DEFAULT %s" % self.__SQL__(self.default)
        return " ".join([i for i in [name_part, dtype_part, nullable_part, default_part] if i])
    
    """
    由于Select API中的where()使用比较运算符将column与值进行比较, 所以我们定义了
    column与值的比较运算返回一个SQL语句的字符串。
    Select([columns]).where(column operator value) method
    
    例如 column_name >= datetime.date(2000,1,1) 则会返回一个 _Select_config对象,
    其中_Select_config.sqlcmd = "column_name >= '2000-01-01'"
    """
    
    def __lt__(self, other):
        return _Select_config("%s < %s" % (self.column_name, self.__SQL__(other) ) )

    def __le__(self, other):
        return _Select_config("%s <= %s" % (self.column_name, self.__SQL__(other) ) )
    
    def __eq__(self, other):
        if other == None: # if Column == None, means column_name is Null
            return _Select_config("%s IS NULL" % self.column_name)
        else:
            return _Select_config("%s = %s" % (self.column_name, self.__SQL__(other) ) )
        
    def __ne__(self, other):
        if other == None: # if Column != None, means column_name NOT Null
            return _Select_config("%s NOT NULL" % self.column_name)
        else:
            return _Select_config("%s != %s" % (self.column_name, self.__SQL__(other) ) )
        
    def __gt__(self, other):
        return _Select_config("%s > %s" % (self.column_name, self.__SQL__(other) ) )
    
    def __ge__(self, other):
        return _Select_config("%s >= %s" % (self.column_name, self.__SQL__(other) ) )
    
    def between(self, lowerbound, upperbound):
        """WHERE...BETWEEN...AND... clause
        """
        return _Select_config("%s BETWEEN %s AND %s" % (self.column_name,
                                                        self.__SQL__(lowerbound),
                                                        self.__SQL__(upperbound),))

    def like(self, wildcards):
        """WHERE...LIKE... clause
        """
        return _Select_config("%s LIKE %s" % (self.column_name, self.__SQL__(wildcards)))

    def in_(self, candidates):
        """WHERE...IN... clause
        """
        return _Select_config("%s IN (%s)" % (self.column_name,
            ", ".join([self.__SQL__(candidate) for candidate in candidates])
            ))
    ## for Update().values() method. example: Update.values(column_name = column_name + 100)
    """
    由于在Update API中的values()方法使用计算符对column进行设定, 所以我们定义了
    column与值的计算返回一个SQL语句的字符串
    Update(table).values(column1 = value1, column2 = value2, ...)
    
    例如 column_name = datetime.date(2000,1,1) 则会返回一个 _Update_config对象,
    其中_Update_config.sqlcmd = "column_name = '2000-01-01'"
    """
    
    def __add__(self, other):
        if isinstance(other, Column):
            return _Update_config("%s + %s" % (self.column_name, other.column_name) )
        else:
            return _Update_config("%s + %s" % (self.column_name, self.__SQL__(other)) )
    
    def __sub__(self, other):
        if isinstance(other, Column):
            return _Update_config("%s - %s" % (self.column_name, other.column_name) )
        else:
            return _Update_config("%s - %s" % (self.column_name, self.__SQL__(other)) )
    
    def __mul__(self, other):
        if isinstance(other, Column):
            return _Update_config("%s * %s" % (self.column_name, other.column_name) )
        else:
            return _Update_config("%s * %s" % (self.column_name, self.__SQL__(other)) )
    
    def __truediv__(self, other):
        if isinstance(other, Column):
            return _Update_config("%s / %s" % (self.column_name, other.column_name) )
        else:
            return _Update_config("%s / %s" % (self.column_name, self.__SQL__(other)) )
    
    def __pos__(self):
        return _Update_config("- %s" % self.column_name)
    
    def __neg__(self):
        return _Update_config("+ %s" % self.column_name)
    

##################################################
#                                                #
#                  Table class                   #
#                                                #
##################################################

class Table():
    """Represent a table in a database.

    e.g.::
        mytable = Table("mytable", metadata,
                    Column("mytable_id", INTEGER(), primary_key=True),
                    Column("value", TEXT()),
                    )

    columns can be accessed by table.column_name

    e.g.::
        mytable_id = mytable.mytable_id

    """
    def __init__(self, table_name, metadata, *args):
        self.table_name = table_name
        self.columns = OrderedDict()
        self.primary_key_columns = list()
        self.pickletype_columns = list()
        
        for column in args:
            # 将column与table绑定后, column就会多出两个table_name和full_name的属性
            column.table_name = self.table_name
            column.full_name = "%s.%s" % (self.table_name, column.column_name)
            # 分别为columns, primary_key_columns, pickletype_columns属性填充数据
            self.columns[column.column_name] = column
            if column.primary_key:
                self.primary_key_columns.append(column.column_name)
            if column.is_pickletype:
                self.pickletype_columns.append(column.column_name)
        
        self.all = list(self.columns.values() );
                
        metadata.tables[self.table_name] = self
        
    def __str__(self):
        return self.table_name
    
    def __repr__(self):
        return "Table('%s', MetaData(), \n\t%s\n\t)" % (self.table_name, 
                                                        ",\n\t".join([repr(column) for column in self.columns.values()]))
    
    
    def __getattr__(self, attr):
        if attr in self.columns:
            return self.columns[attr]
        else:
            raise AttributeError
    
    def create_table_sql(self):
        """generate the 'CREATE TABLE...' SQL command
        
        example output:
        
            CREATE TABLE movie (
                movie_id TEXT,
                title TEXT DEFAULT 'unknown_title',
                length INTEGER DEFAULT -1,
                release_date DATE DEFAULT '0000-01-01',
                genres TEXT DEFAULT 'gANjYnVpbHRpbnMKc2V0CnEAXXEBhXECUnEDLg==',
                PRIMARY KEY (movie_id));
        """
        cmd_CREATE_TABLE = "CREATE TABLE %s" % self.table_name
        cmd_DATATYPE = ",\n\t".join([column.create_table_sql() for column in self.columns.values()])
        
        primary_key_columns = [column.column_name for column in self.columns.values() if column.primary_key]
        if len(primary_key_columns) == 0:
            cmd_PRIMARY_KEY = ""
        else:
            cmd_PRIMARY_KEY = ",\n\tPRIMARY KEY (%s)" % ", ".join(primary_key_columns)
            
        template = "%s (\n\t%s%s);" 
        return template % (cmd_CREATE_TABLE,
                           cmd_DATATYPE,
                           cmd_PRIMARY_KEY,)
    
    def insert(self):
        """create a Insert object
        """
        return Insert(self)

    def update(self):
        """create a Update object
        """
        return Update(self)

    def drop(self, engine):
        """drop a table from database that bind with engine regardless if it is existing.
        """
        try:
            engine.execute("DROP TABLE %s" % self.table_name)
        except:
            pass
        
##################################################
#                                                #
#              Sqlite3Engine class               #
#                                                #
##################################################

class Sqlite3Engine():
    def __init__(self, dbname, autocommit=True):
        self.dbname = dbname
        self.is_autocommit = autocommit
        
        sqlite3.register_adapter(list, adapt_list)
        sqlite3.register_converter("PYTHONLIST", convert_list)
        
        sqlite3.register_adapter(set, adapt_set)
        sqlite3.register_converter("PYTHONSET", convert_set)
        
        sqlite3.register_adapter(dict, adapt_dict)
        sqlite3.register_converter("PYTHONDICT", convert_dict)
        
        sqlite3.register_adapter(OrderedDict, adapt_ordereddict)
        sqlite3.register_converter("ORDEREDDICT", convert_ordereddict)

        sqlite3.register_adapter(StrSet, StrSet.sqlite3_adaptor)
        sqlite3.register_converter("STRSET", StrSet.sqlite3_converter)
        
        sqlite3.register_adapter(IntSet, IntSet.sqlite3_adaptor)
        sqlite3.register_converter("INTSET", IntSet.sqlite3_converter)

        sqlite3.register_adapter(StrList, StrList.sqlite3_adaptor)
        sqlite3.register_converter("STRLIST", StrList.sqlite3_converter)
        
        sqlite3.register_adapter(IntList, IntList.sqlite3_adaptor)
        sqlite3.register_converter("INTLIST", IntList.sqlite3_converter)

        self.connect = sqlite3.connect(dbname, detect_types=sqlite3.PARSE_DECLTYPES)
        self.cursor = self.connect.cursor()
        
        if self.is_autocommit:
            self._commit = self.commit
        else:
            self._commit = self.commit_nothing

    def execute(self, *args, **kwarg):
        return self.cursor.execute(*args, **kwarg)
    
    def commit(self):
        """method for manually commit operation
        """
        self.connect.commit()

    def commit_nothing(self):
        """method for doing nothing
        """
        pass

    def autocommit(self, flag):
        """switch on or off autocommit
        """
        if flag:
            self.is_autocommit = True
            self._commit = self.commit
        else:
            self.is_autocommit = False
            self._commit = self.commit_nothing
            
    ### === Insert ===
    def insert_record(self, insert_obj, record):
        """插入单条记录"""
        insert_obj.sqlcmd_from_record()
        self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_record_converter(record))
        self._commit()

    def insert_many_records(self, insert_obj, records):
        """插入多条记录"""
        insert_obj.sqlcmd_from_record()
        for record in records:
            try:
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_record_converter(record))
            except:
                pass
        self._commit()
        
    def insert_row(self, insert_obj, row):
        """插入单条Row object"""
        insert_obj.sqlcmd_from_row(row)
        self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_row_converter(row))
        self._commit()
    
    def _insert_many_rows_list_mode(self, insert_obj, rows):
        """内部函数, 以列表模式插入多条Row object"""
        row = rows[0]
        insert_obj.sqlcmd_from_row(row)
        if set(row.columns).isdisjoint(set(insert_obj.table.pickletype_columns)): # 如果没有交集
            insert_obj.current_converter = insert_obj.nonpicklize_row
        else: # 如果有交集, 要用到picklize_row
            insert_obj.current_converter = insert_obj.picklize_row
        
        for row in rows:
            try:
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.current_converter(row))
            except:
                pass
        self._commit()
                
    def _insert_many_rows_generator_mode(self, insert_obj, rows):
        """内部函数, 以生成器模式插入多条Row object"""
        row = next(rows)
        insert_obj.sqlcmd_from_row(row)
        if set(row.columns).isdisjoint(set(insert_obj.table.pickletype_columns)): # 如果没有交集
            insert_obj.current_converter = insert_obj.nonpicklize_row
        else: # 如果有交集, 要用到picklize_row
            insert_obj.current_converter = insert_obj.picklize_row
            
        try: # manually insert the first element of the generator
            self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.current_converter(row))
        except:
            pass
        
        for row in rows:
            try:
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.current_converter(row))
            except:
                pass
        self._commit()

    def insert_many_rows(self, insert_obj, rows):
        """自动判断以生成器模式还是列表模式插入多条Row object
        """
        try:
            self._insert_many_rows_generator_mode(insert_obj, rows)
        except:
            self._insert_many_rows_list_mode(insert_obj, rows)
    
    ### === insert and update ===
    def insert_and_update_many_records(self, insert_obj, records):
        update_obj = insert_obj.table.update()
        insert_obj.sqlcmd_from_record()
        
        for record in records: # try insert one by one
            try: # try insert
                self.cursor.execute(insert_obj.insert_sqlcmd, 
                                    insert_obj.default_record_converter(record))
            except sqlite3.IntegrityError:
                values_kwarg = dict() # update.values()'s argument
                where_args = list() # update.values().where()'s argument
                 
                for column_name, column, value in zip(update_obj.table.columns,
                                                      update_obj.table.columns.values(),
                                                      record):
                    values_kwarg[column_name] = value # fill in values dictionary
                    if column.primary_key: # use primary_key value to locate the row to update
                        where_args.append( column == value)
                
                # update one
                self.update( update_obj.values(**values_kwarg).where(*where_args) )
                
        self._commit()

    def insert_and_update_many_rows(self, insert_obj, rows):
        update_obj = insert_obj.table.update()
        
        for row in rows: # try insert one by one
            try: # try insert
                insert_obj.sqlcmd_from_row(row)
                self.cursor.execute(insert_obj.insert_sqlcmd, insert_obj.default_row_converter(row))
                
            except sqlite3.IntegrityError:
                values_kwarg = dict() # update.values()'s argument
                where_args = list() # update.values().where()'s argument
                
                for column_name, value in zip(row.columns, row.values):
                    column = update_obj.table.columns[column_name]
                    values_kwarg[column_name] = value # fill in values dictionary
                    if column.primary_key:
                        where_args.append( column == value)
                
                # update one
                self.update( update_obj.values(**values_kwarg).where(*where_args) )
                
        self._commit()
        
    ### === Select ===
    def select(self, select_obj):
        """以生成器形式返回行数据
        """
        for record in self.cursor.execute(select_obj.toSQL()):
            yield select_obj.default_record_converter(record)
            
    def select_row(self, select_obj):
        """以生成器形式返回封装成Row对象的行数据
        """
        for record in self.select(select_obj):
            row = Row(select_obj.column_names, record)
            yield row
    
    def select_column(self, select_obj):
        """返回一个封装好的列表
        """
        dataframe = OrderedDict()
        for column_name in select_obj.column_names:
            dataframe[column_name] = list()
        for record in self.select(select_obj):
            for column_name, value in zip(select_obj.column_names, record):
                dataframe[column_name].append(value)
        return dataframe

    ### === Update ===
    def update(self, update_obj):
        """更新数据
        """
        update_obj.sqlcmd()
        self.cursor.execute(update_obj.update_sqlcmd)
        self._commit()
    
    ### === 一些简便的语法糖函数 ===
    def select_all(self, table):
        """选择整个表
        """
        return self.select(Select(table.all))
    
    def prt_all(self, table):
        """打印整个表
        """
        counter = 0
        for record in self.select_all(table):
            print(record)
            counter += 1
        print("Found %s records in %s" % (counter, table.table_name))
    
    def howmany(self, table):
        """返回表内的记录总数
        """
        self.cursor.execute("SELECT COUNT(*) FROM (SELECT * FROM %s);" % table.table_name)
        return self.cursor.fetchone()[0]
    
    def prt_howmany(self, table):
        """打印表内有多少条记录
        """
        num_of_record = self.howmany(table)
        print("Found %s records in %s" % (num_of_record, table.table_name))

if __name__ == "__main__":
    import unittest
    from angora.STRING import *
    from angora.DATA import *
    from datetime import datetime, date, timedelta
    from pprint import pprint as ppt
    import random
    
    engine = Sqlite3Engine(":memory:", autocommit=False)
    metadata = MetaData()
    dtype = DataType()
    test = Table("test", metadata,
        Column("integer_type", dtype.integer, primary_key=True),
        Column("real_type", dtype.real, default=-1),
        Column("text_type", dtype.text, default="Nothing"),
        Column("date_type", dtype.date, default=date(2014,1,1)),
        Column("datetime_type", dtype.datetime, default=datetime(2014,1,1,0,0,0)),
        Column("pickle_type", dtype.pickletype, default={"测试": 123}),
        Column("strlist_type", dtype.strlist, default=StrList(["aaa", "bbb", "ccc"])),
        Column("intlist_type", dtype.intlist, default=IntList([111, 222, 333])),
        Column("strset_type", dtype.strset, default=StrSet({"aaa", "bbb", "ccc"})),
        Column("intset_type", dtype.intset, default=IntSet({111, 222, 333})),
        )
    metadata.create_all(engine)
    
    ins = test.insert()
    rows = list()
    for i in range(10):
        row = dict()
        row["integer_type"] = i
        row["real_type"] = random.random()
        row["text_type"] = fmter.tpl.randstr(32)
        row["date_type"] = timewrapper.randdate("2014-01-01", "2014-12-31")
        row["datetime_type"] = timewrapper.randdatetime("2014-01-01 00:00:00", "2014-12-31 23:59:59")
        row["pickle_type"] = {1: "a", 2: "b", 3: "c"}
        row["strlist_type"] = StrList(["a", "b", "c"])
        row["intlist_type"] = IntList([1, 2, 3])
        row["strset_type"] =  StrSet({"a", "b", "c"})
        row["intset_type"] =  IntSet({1, 2, 3})
        rows.append(Row.from_dict(row))
    
    rows.append(Row.from_dict({"integer_type": 10}))
    engine.insert_many_rows(ins, rows)
    

    
    ### ========== Put temp code here ============

    class ColumnUnittest(unittest.TestCase):
        def test_sql(self):
            self.assertEqual(test.integer_type._sql_STRING_NUMBER(100), "100")
            self.assertEqual(test.integer_type._sql_STRING_NUMBER("test"), "'test'")
            self.assertEqual(test.integer_type._sql_DATE(date(2015, 1, 1)), "'2015-01-01'")
            self.assertEqual(test.integer_type._sql_DATETIME(datetime(2015, 1, 1, 0, 6, 30)), "'2015-01-01 00:06:30'")
            self.assertEqual(test.integer_type._sql_PICKLETYPE({1: "a", 2: "b"}), "X'80037d7100284b0158010000006171014b025801000000627102752e'")
            self.assertIn(test.integer_type._sql_STRSET({"01", "02"}), ["'01&&02'", "'02&&01'"])
            self.assertIn(test.integer_type._sql_INTSET({1, 2}), ["'1&&2'", "'2&&1'"])
            self.assertEqual(test.integer_type._sql_STRLIST(["01", "02", "03"]), "'01&&02&&03'")
            self.assertEqual(test.integer_type._sql_INTLIST([1, 2, 3]), "'1&&2&&3'")
        
        def test_create_table_sql(self):
            col = Column("integer_type", INTEGER())
            self.assertEqual(col.create_table_sql(), "integer_type INTEGER")
            col = Column("datetime_type", DATETIME())
            self.assertEqual(col.create_table_sql(), "datetime_type TIMESTAMP")
            col = Column("pickle_type", PICKLETYPE())
            self.assertEqual(col.create_table_sql(), "pickle_type BLOB")
                
        def test_comparison_operation(self):
            # 测试 >, <, >=, <=, ==, !=, between, like 八大比较运算符运算返回的是否是_Select_config
            self.assertTrue(isinstance(test.integer_type >= 0, _Select_config))
            self.assertTrue(isinstance(test.text_type <= "abc", _Select_config))
            self.assertTrue(isinstance(test.date_type > date.today(), _Select_config))
            self.assertTrue(isinstance(test.datetime_type < datetime.now(), _Select_config))
            self.assertTrue(isinstance(test.strlist_type == ["a", "b", "c"], _Select_config))
            self.assertTrue(isinstance(test.intlist_type != [1, 2, 3], _Select_config))
            self.assertTrue(isinstance(test.real_type.between(0, 1), _Select_config))
            self.assertTrue(isinstance(test.text_type.like("%pattern%"), _Select_config))
            self.assertTrue(isinstance(test.intset_type.in_(({1,2,3}, {4,5,6}, {7,8,9})), _Select_config))
            
            # 测试 >, <, >=, <=, ==, !=, between, like 八大比较运算符返回的sqlcmd
            self.assertEqual((test.integer_type >= 0).sqlcmd, "integer_type >= 0")
            self.assertEqual((test.text_type <= "abc").sqlcmd, "text_type <= 'abc'")
            self.assertEqual((test.date_type > date(2015, 1, 1)).sqlcmd, "date_type > '2015-01-01'")
            self.assertEqual((test.datetime_type < datetime(2015, 1, 1, 0, 6, 30)).sqlcmd, "datetime_type < '2015-01-01 00:06:30'")
            self.assertEqual((test.strlist_type == ["a", "b", "c"]).sqlcmd, "strlist_type = 'a&&b&&c'")
            self.assertEqual((test.intlist_type != [1, 2, 3]).sqlcmd, "intlist_type != '1&&2&&3'")
            self.assertEqual((test.real_type.between(0, 1)).sqlcmd, "real_type BETWEEN 0 AND 1")
            self.assertEqual((test.text_type.like("%pattern%")).sqlcmd, "text_type LIKE '%pattern%'")
            self.assertEqual((test.intset_type.in_(({1,2,3}, {4,5,6}, {7,8,9}))).sqlcmd, "intset_type IN ('1&&2&&3', '4&&5&&6', '8&&9&&7')")
            
            # 测试 !=, == 和None做比较的时候, 是否能转化为SQL中的
            self.assertEqual((test.text_type == None).sqlcmd, "text_type IS NULL")
            self.assertEqual((test.text_type != None).sqlcmd, "text_type NOT NULL")
        
        def test_calculation_operation(self):
            # 测试 +, -, *, /, pos, neg,
            self.assertTrue(isinstance(test.integer_type + 1, _Update_config))
            self.assertTrue(isinstance(test.integer_type - 1, _Update_config))
            self.assertTrue(isinstance(test.integer_type * 2.0, _Update_config))
            self.assertTrue(isinstance(test.integer_type / 2.0, _Update_config))
            self.assertTrue(isinstance(- test.integer_type, _Update_config))
            self.assertTrue(isinstance(+ test.integer_type, _Update_config))
            
            
            self.assertEqual((test.integer_type + 1).sqlcmd, "integer_type + 1")
            self.assertEqual((test.integer_type - 1).sqlcmd, "integer_type - 1")
            self.assertEqual((test.integer_type * 2.0).sqlcmd, "integer_type * 2.0")
            self.assertEqual((test.integer_type / 2.0).sqlcmd, "integer_type / 2.0")
            self.assertEqual((-test.integer_type).sqlcmd, "+ integer_type")
            self.assertEqual((+test.integer_type).sqlcmd, "- integer_type")
            
        def test_sql_value(self):
            self.assertEqual(test.integer_type.__SQL__(1), "1")
            self.assertEqual(test.real_type.__SQL__(1.1), "1.1")
            self.assertEqual(test.text_type.__SQL__("abc"), "'abc'")
            self.assertEqual(test.date_type.__SQL__(date(2014,1,1)), "'2014-01-01'")
            self.assertEqual(test.datetime_type.__SQL__(datetime(2014,1,1,0,6,30)), "'2014-01-01 00:06:30'")
            self.assertEqual(test.pickle_type.__SQL__([1,2,3]), "X'80035d7100284b014b024b03652e'")
            self.assertEqual(test.strlist_type.__SQL__(StrList(["a","b","c"])), "'a&&b&&c'")
            
    class SqliteEngineUnittest(unittest.TestCase):
        # === select, select_row, select_column ===
        def test_select(self):
            """测试select能否返回record tuple
            """
            results = list(engine.select(Select(test.all)) )
            self.assertEqual(results[0][0], 0)
            self.assertDictEqual(results[1][5], {1: "a", 2: "b", 3: "c"})
            self.assertListEqual(results[2][6], StrList(["a", "b", "c"]))
            self.assertSetEqual(results[3][9], IntSet({1, 2, 3}))
        
        def test_select_row(self):
            """测试select能否返回Row对象, 即可以用Row.key或Row[key]的方法获得值
            """
            results = list(engine.select_row(Select(test.all)) )
            self.assertEqual(results[0].integer_type, 0)
            self.assertDictEqual(results[1].pickle_type, {1: "a", 2: "b", 3: "c"})
            self.assertListEqual(results[2]["strlist_type"], StrList(["a", "b", "c"]))
            self.assertSetEqual(results[3]["intset_type"], IntSet({1, 2, 3}))
            
        def test_select_column(self):
            """测试select_column是否能返回一个类似pandas.DataFrame的以列为导向的视图
            """
            df = engine.select_column(Select([test.integer_type]))
            self.assertListEqual(df["integer_type"], list(range(10)))
            
        def test_where(self):
            results = list(engine.select_row(
                        Select(test.all).\
                        where(test.text_type == None)
                        ))
            print(results)
        # === select, where ===
    class InsertUnittest(unittest.TestCase):
        def test_sqlcmd(self):
            """测试Insert是否能够根据不同的record, Row自动判断其中的类型, 然后生成用于插入到
            数据库中的SQL语句, 以及把pickletype的列的值转化成bytestr
            """
            ins = test.insert()
            record = (11, 1.0, "abc", date(2015, 1, 1), datetime(2015, 1, 1, 0, 0, 0),
                      [1, 2, 3], StrList(["c", "b", "a"]), IntList([3, 2, 1]),
                      StrSet(["c", "b", "a"]), IntSet([3, 2, 1]))
            row = Row(tuple(test.columns.keys()),
                      record)
            
            correct_sqlcmd = "INSERT INTO test\n\t(integer_type, real_type, text_type, date_type, datetime_type, pickle_type, strlist_type, intlist_type, strset_type, intset_type)\nVALUES\n\t(?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            
            # test sqlcmd_from_record() method
            ins.sqlcmd_from_record()
            self.assertEqual(ins.insert_sqlcmd, correct_sqlcmd)
            
            # test sqlcmd_from_row() method
            ins.sqlcmd_from_row(row)
            self.assertEqual(ins.insert_sqlcmd, correct_sqlcmd)
            
            # test picklize_record() method
            self.assertTupleEqual(ins.picklize_record(record),
                (11, 1.0, 'abc', date(2015, 1, 1), datetime(2015, 1, 1, 0, 0), 
                 b"\x80\x03]q\x00(K\x01K\x02K\x03e.", 
                 ['c', 'b', 'a'], [3, 2, 1], StrSet({'a', 'c', 'b'}), IntSet({1, 2, 3}))
                )
            
            # test picklize_row() method
            self.assertTupleEqual(ins.picklize_row(row),
                (11, 1.0, 'abc', date(2015, 1, 1), datetime(2015, 1, 1, 0, 0), 
                 b"\x80\x03]q\x00(K\x01K\x02K\x03e.", 
                 ['c', 'b', 'a'], [3, 2, 1], StrSet({'a', 'c', 'b'}), IntSet({1, 2, 3}))
                )
    
    class UpdateUnittest(unittest.TestCase):
        def test_sqlcmd(self):
            pass
        
    class RowUnittest(unittest.TestCase):
        def test_initiate(self):
            """测试Row的两种初始化方式, 字典视图, 对值的两种访问方式的测试
            """
            row1 = Row( ("text_type", "integer_type"), ( "abc", 1,))
            row2 = Row.from_dict({"integer_type": 1, "text_type": "abc"})
            
            # test __str__() method
            self.assertEqual(str(row1), "OrderedDict([('text_type', 'abc'), ('integer_type', 1)])")
            
            # test __repr__() method
            self.assertEqual(repr(row1), "Row(columns=('text_type', 'integer_type'), values=('abc', 1))")
            
            # test __getattr__() method
            self.assertEqual(row1.text_type, "abc")
            
            # test __getitem__() method
            self.assertEqual(row1["integer_type"], 1)
            
            # test __eq__() method
            self.assertEqual(row1, row2)
            
            # test __setitem__() method
            row2["integer_type"] = 2
            self.assertEqual(row2["integer_type"], 2)
            
            # from_dict(), _create_dict_view(), _smart_create_dict_view() are implicitly tested
    
    unittest.main()