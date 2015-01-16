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

  '''等価な整数にキャスト可能ならする'''
  def simplify(self, margin = 0):
    self.src = getNumber(self.src, margin)
    self.trg = getNumber(self.trg, margin)
    self.co  = getNumber(self.co,  margin)

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

  def getReversed(self):
    recRev = self.__class__()
    recRev.src = self.trg
    recRev.trg = self.src
    recRev.counts = self.counts.getReversed()
    recRev.aligns = getRevAligns(self.aligns)
    revFeatures = {}
    if 'egfp' in self.features:
      revFeatures['fgep'] = self.features['egfp']
    if 'egfl' in self.features:
      revFeatures['fgel'] = self.features['egfl']
    if 'fgep' in self.features:
      revFeatures['egfp'] = self.features['fgep']
    if 'fgel' in self.features:
      revFeatures['egfl'] = self.features['fgel']
    if 'p' in self.features:
      revFeatures['p'] = self.features['p']
    revFeatures['w'] = len(self.srcTerms)
    recRev.features = revFeatures
    return recRev


class MosesRecord(Record):
  def __init__(self, line = "", split = '|||'):
    Record.__init__(self)
    self.split = split
    self.loadLine(line, split)

  def loadLine(self, line, split = '|||'):
    if line:
      fields = line.strip().split(split)
      self.src = fields[0].strip()
      self.trg = fields[1].strip()
      self.features = getMosesFeatures(fields[2])
      self.aligns = fields[3].strip().split()
      listCounts = getCounts(fields[4])
      self.counts.setCounts(trg = listCounts[0], src = listCounts[1], co = listCounts[2])

  def getSrcSymbols(self):
    return self.src.split(' ')
  srcSymbols = property(getSrcSymbols)

  def getSrcTerms(self):
    return self.src.split(' ')
  srcTerms = property(getSrcTerms)

  def getTrgSymbols(self):
    return self.trg.split(' ')
  trgSymbols = property(getTrgSymbols)

  def getTrgTerms(self):
    return self.trg.split(' ')
  trgTerms = property(getTrgTerms)

  def toStr(self, s = ' ||| '):
    strFeatures = getStrMosesFeatures(self.features)
    strAligns = str.join(' ', self.aligns)
    self.counts.simplify(0.0001)
    strCounts   = "%s %s %s" % (self.counts.trg, self.counts.src, self.counts.co)
    buf = str.join(s, [self.src, self.trg, strFeatures, strAligns, strCounts]) + "\n"
    return buf


def getAlignMap(aligns, reverse = False):
  alignMap = {}
  for align in aligns:
    (s, t) = map(int, align.split('-'))
    if reverse:
      alignMap.setdefault(t, []).append(s)
    else:
      alignMap.setdefault(s, []).append(t)
  return alignMap

def getCounts(field):
  return map(getNumber, field.split())

def getNumber(anyNum, margin = 0):
  numFloat = float(anyNum)
  numInt = int(numFloat)
  if margin > 0:
    if abs(numInt - numFloat) < margin:
      return numInt
    elif abs(numInt+1 - numFloat) < margin:
      return numInt+1
    elif abs(numInt-1 - numFloat) < margin:
      return numInt-1
    else:
      return numFloat
  elif numFloat == numInt:
    return numInt
  else:
    return numFloat

def getRevAligns(aligns):
  revAlignList = []
  for a in aligns:
    (s, t) = map(int, a.split('-'))
    revAlignList.append( "%d-%d" % (t, s) )
  return sorted(revAlignList)


def getMosesFeatures(field):
  features = {}
  scores = map(getNumber, field.split())
  features['fgep'] = scores[0]
  features['fgel'] = scores[1]
  features['egfp'] = scores[2]
  features['egfl'] = scores[3]
  return features

def getStrMosesFeatures(dicFeatures):
  '''素性辞書を、スペース区切りのスコア文字列に戻す'''
  scores = []
  scores.append( dicFeatures.get('fgep', 0) )
  scores.append( dicFeatures.get('fgel', 0) )
  scores.append( dicFeatures.get('egfp', 0) )
  scores.append( dicFeatures.get('egfl', 0) )
  return str.join(' ', map(str,scores))

