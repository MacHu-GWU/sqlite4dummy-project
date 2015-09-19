#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
This module provides some high performance iterator function support.

class, method, func, exception
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
"""

import itertools
import sys

is_py2 = (sys.version_info[0] == 2)
if is_py2:
    from itertools import ifilterfalse as filterfalse, izip_longest as zip_longest
else: # in python3
    from itertools import filterfalse, zip_longest
    
def grouper(iterable, n, fillvalue=None):
    """Collect data into fixed-length chunks or blocks.
    
    Usage::
    
        >>> list(grouper(range(10), n=3, fillvalue=1024))
        [(0, 1, 2), (3, 4, 5), (6, 7, 8), (9, 1024, 1024)]
    """
    # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
    args = [iter(iterable)] * n
    return zip_longest(fillvalue=fillvalue, *args)

def grouper_list(LIST, n):
    """Evenly divide LIST into fixed-length piece, no filled value if chunk 
    size smaller than fixed-length.
    
    Usage::
    
        >>> list(grouper(range(10), n=3)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9]]
    """
    for group in grouper(LIST, n):
        chunk_l = list()
        for i in group:
            if i:
                chunk_l.append(i)
        yield chunk_l