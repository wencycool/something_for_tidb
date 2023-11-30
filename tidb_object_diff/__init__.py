# -*- coding: utf-8 -*-
# coding: utf-8

import sys

# 判断python的版本
if float(sys.version[:3]) < 3.6:
    raise Exception("python version need larger than 3.6")
