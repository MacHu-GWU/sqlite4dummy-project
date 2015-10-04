#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

if sys.version_info[0] == 3:
    _str_type = str
    _int_types = (int,)
    PK_PROTOCOL = 3
    is_py3 = True
else:
    _str_type = basestring
    _int_types = (int, long)
    PK_PROTOCOL = 2
    is_py3 = False