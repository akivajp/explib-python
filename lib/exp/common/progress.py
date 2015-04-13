#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''処理途中の内容をコンソールに動的に書き換える'''

import codecs
import os
import sys
import time

_stdout = sys.stdout
if sys.version_info.major < 3:
  _stdout = codecs.getwriter('utf-8')(sys.stdout)

def _clean(n = 1):
  if n > 0:
    sys.stdout.write(' ' * n + "\b" * n)

if sys.version_info.major >= 3:
  def _len(buf):
    return len( bytes(buf, 'utf-8') )
else:
  def _len(buf):
    return len( buf )

if sys.version_info.major >= 3:
  def _str(arg):
    return str(arg)
else:
  def _str(arg):
    if type(arg) == str:
      return arg.decode('utf-8')
    else:
      return unicode(arg)

_lastPos = 0
def log(*args, **keys):
  '''処理途中の内容をコンソールに動的に書き換える'''
  global _lastPos
  global _resume
  if not 'sep' in keys:
    keys['sep'] = ' '
  _stdout.write("\r")
  strTime = time.strftime('[%Y/%m/%d %H:%M:%S] ')
  buf = strTime + keys['sep'].join( map(_str, args) )
  _stdout.write( buf )
  count = _len(buf)
  _clean( (_lastPos - count) * 2 )
  _lastPos = count
  _stdout.flush()

class Counter(object):
  '''プログレス表示更新のタイミングを示すカウンター

  カウント数が閾値を超えると #should_print() がTrueを返す。
  #update()を呼び出すと、閾値が unit 分だけ増加し、
  閾値が unit * scaleup を超えると、unit が10倍される。
  limit を設定していると、ratioプロパティが割合を返す。
  '''
  def __init__(self, scaleup = 1000, limit = -1):
    self._count = 0
    self._limit = limit
    self._scaleup = scaleup
    self._threshold = 1
    self._unit = 1

  def add(self, count = 1):
    self._count += count
    return self.shouldPrint()

  def _getCount(self):
    return self._count
  def _setCount(self, val):
    self._count = val
  def _delCount(self):
    self._count = 0
    self._unit  = 1
  count = property(_getCount, _setCount, _delCount)

  def _getLimit(self):
    return self._limit
  def _setLimit(self, limit):
    self._limit = limit
  def _delLimit(self):
    self._limit = -1
  limit = property(_getLimit, _setLimit, _delLimit)

  def _getRatio(self):
    return self._count / float(self._limit)
  ratio = property(_getRatio)

  def set(self, count = 0):
    self._count = count

  def shouldPrint(self):
    return self._count > self._threshold

  def update(self):
    self._threshold += self._unit
    if self._threshold >= self._unit * self._scaleup:
      self._unit *= 10

  def _getUnit(self):
    return self._unit
  unit = property(_getUnit)

