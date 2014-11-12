#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''キャッシュ機能を実現するクラス群'''

from collections import OrderedDict

'''任意の件数のデータを保持し、最後に使われたデータから削除していく辞書クラス'''
class Cache(OrderedDict):
  def __init__(self, maxlen = 100):
    OrderedDict.__init__(self)
    self._maxlen = maxlen

  def __setitem__(self, key, value):
    if key in self:
      del self[key]
    OrderedDict.__setitem__(self, key, value)
    if len(self) > self._maxlen:
      self.popitem(last = False)

  def use(self, key):
    if key in self:
      value = OrderedDict.__getitem__(self, key)
      OrderedDict.__delitem__(self, key)
      OrderedDict.__setitem__(self, key, value)
      return value
    else:
      return None

