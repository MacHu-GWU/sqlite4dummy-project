sqlite4dummy, a high performance and easy to use Sqlite API for Data Scientist
===================================================================================================

Some quick link:

- `Documentation <http://sqlite4dummy-project.readthedocs.org/>`_
- `PyPI download <https://pypi.python.org/pypi/sqlite4dummy>`_

Since Sqlalchemy is the best, why another Sqlite API?
---------------------------------------------------------------------------------------------------

We already have an awesome project `Sqlalchemy <http://www.sqlalchemy.org/>`_,
Why I rebuild the wheel again? And why only `Sqlite <https://www.sqlite.org/>`_?

1. In bulk insert operation, sometime we meet primary key conflict. In this scenario, we have to insert records one by one, catch the exception and handle it. Because Sqlalchemy is created to be compatible with most of database system, the way Sqlalchemy handle the exception is rollback. But, sqlite is so special. In sqlite, there's only one writer is allowed at one time, and there's no transaction. That's why sqlite don't need the rollback mechanism. In the sqlite Python generic API, we can simple pass that exception. As result, **the generic API is 50-100 times faster than Sqlalchemy** when there's conflict in bulk insert.

2. Sqlalchemy use Rowproxy to preprocess the data that cursor returns. After that, we can visit value by the column name. But sometime, we actually don't need this feature. A better way is to activate this feature when we need it. That makes **Sqlalchemy is 1.5 to 3 times slower generic API**.

sqlite is an excellent database product. Single file, C API, ultra fast. All these features is what a Data Scientist dream of. It's perfect for analysis on mid size dataset (from 1GB to 1TB). Specifically when tons of selection operation are needed.

Plus, with very small lose of features (missing features are usually useless in analytic work.), the new sqlite4dummy Sqlite API is optimized. The interface is almost a human language. **Which allows non-programming, non-database background statistician, analyist to take advantage of high performance I/O of data from Sqlite.**

At the end, if you don't need transaction, user group and love high performance in batch operations, use sqlite4dummy. Enjoy it!


已经有了Sqlalchemy, 为啥还需要sqlite4dummy?
---------------------------------------------------------------------------------------------------

为什么我们在已经有Sqlalchemy(下称SA)这么优秀的项目的情况下, 我们还要仅仅为sqlite
做一个新的API呢?

SA为了能够兼容所有主流关系数据库, 所以牺牲了一些性能。虽然SA的功能无比强大, 但是
在一些特殊情况下, 并不能给我们带来任何的利益。特别是在数据分析中。

对于数据科学家而言, Sqlite是一个非常适合加速IO的数据库。单文件, C实现, 简单高速,
这些特性都非常适合分析中等大小(1GB - 1TB)的数据集。而Transaction, Session, User
Group这些功能, 我们并不需要。

此外, SA在性能上有两个致命的弱点:

1. SA在执行Select的时候, 调用了一种叫做Rowproxy的机制, 将所有的行打包成字典, 方便
我们进行读取。这一特性我们并不是100%的需要, 而我们完全可以在需要的时候, 再打包
成字典。这使得SA在Select返回大量数据的情况下, 要比原生API慢50%左右。

2. SA在执行Insert的时候, 如果发生了primary key conflict, 由于SA需要兼容所有的数据库,
所以SA使用了rollback机制。而由于sqlite3只支持单线程的write, 所以在处理冲突的时候
要比多线程简单很多, 导致SA的速度在当写入的数据有冲突的时候, 速度要比原生sqlite
API慢几十倍甚至百倍。

这就是我们重新创造一个面向数据分析人员, 而又提供比SA更加简化直观的API的
sqlite4dummy, 让非计算机背景的人员也能轻松愉快的使用sqlite数据库带来的极大便利。


Install
---------------------------------------------------------------------------------------------------

sqlite4dummy is released on PyPI, so all you need is::

$ pip install sqlite4dummy