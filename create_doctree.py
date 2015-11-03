#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from docfly import Docfly
import os

try:
    os.remove(r"source\sqlite4dummy")
except Exception as e:
    print(e)

docfly = Docfly(
    "sqlite4dummy", 
    dst="source", 
    ignore=[
        "sqlite4dummy.tests", 
        "sqlite4dummy.zzz_manual_install.py",
    ],
)
docfly.fly()