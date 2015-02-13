#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''数値周りの処理'''

def toNumber(anyNum, margin = 0):
    numFloat = float(anyNum)
    numInt = int(round(numFloat))
    if abs(numFloat - numInt) <= margin:
        return numInt
    else:
        return numFloat

