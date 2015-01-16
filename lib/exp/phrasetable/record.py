#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''フレーズテーブルのレコードを扱うクラス'''

class CoOccurrence(object):
  def __init__(self, src = 0, trg = 0, co = 0):
    self.src = src
    self.trg = trg
    self.co  = co

  '''P(e|f)を計算する。（src → trg）'''
  def calcEGFP(self):
    return co / float(src)
  egfp = property(calcEGFP)

  '''P(f|e)を計算する。（trg → src）'''
  def calcFGEP(self):
    return co / float(trg)
  fgep = property(calcFGEP)

  def getReversed(self):
    return CoOccurrence(self.trg, self.src, self.co)

  def setCounts(self, src = None, trg = None, co = None):
    if src:
      self.src = src
    if trg:
      self.trg = trg
    if co:
      self.co = co

  def __str__(self):
    return "CoOccurrence(src = %s, trg = %s, co = %s)" % (src, trg, co)

class Record(object):
  def __init__(self):
    self.src = ""
    self.trg = ""
    self.features = {}
    self.counts = CoOccurrence()
    self.aligns = []

  def getAlignMap(self):
    return getAlignMap(self.aligns, reverse = False)
  alignMap = property(getAlignMap)

  def getAlignMapRev(self):
    return getAlignMap(self.aligns, reverse = True)
  alignMapRev = property(getAlignMapRev)


class MosesRecord(Record):
  pass

def getAlignMap(aligns, reverse = False):
  alignMap = {}
  for align in aligns:
    (s, t) = map(int, align.split('-'))
    if reverse:
      alignMap.setdefault(t, []).append(s)
    else:
      alignMap.setdefault(s, []).append(t)
  return alignMap

def getRevAligns(aligns):
  revAlignList = []
  for a in aligns:
    (s, t) = map(int, a.split('-'))
    revAlignList.append( "%d-%d" % (t, s) )
  return sorted(revAlignList)

