#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''ルールテーブルのレコードを扱うクラス'''

import math

from exp.phrasetable.record import Record
from exp.phrasetable.record import getRevAligns

def getCounts(field):
  return map(getNumber, field.split())

def getFeatures(field):
  features = {}
  for strKeyVal in field.split():
    (key, val) = strKeyVal.split('=')
    val = getNumber(val)
    if key in ['egfl', 'egfp', 'fgel', 'fgep']:
      val = math.e ** val
    features[key] = val
  return features

def getStrFeatures(dic):
  '''素性辞書を、'key=val' という文字列で表したリストに変換する'''
  featureList = []
  for key, val in dic.items():
    if key in ['egfl', 'egfp', 'fgel', 'fgep']:
      try:
        val = math.log(val)
      except:
        print(key, val)
    featureList.append( "%s=%s" % (key, val) )
  return str.join(' ', sorted(featureList))

def getNumber(strNum):
  numFloat = float(strNum)
  numInt = int(numFloat)
  if numFloat == numInt:
    return numInt
  else:
    return numFloat

def getSymbols(rule):
  symbols = []
  for s in rule.split(' '):
    if len(s) < 2:
      if s == "@":
        break
    elif s[0] == '"' and s[-1] == '"':
      symbols.append(s[1:-1])
    elif s[0] == 'x' and s[1].isdigit():
      if len(s) > 3:
        symbols.append('[%s]' % s[3:])
      else:
        symbols.append('[X]')
  return symbols

def getTerms(rule):
  terms = []
  for s in rule.split(' '):
    if len(s) < 2:
      if s == "@":
        break
    elif s[0] == '"' and s[-1] == '"':
      terms.append(s[1:-1])
  return terms

class TravatarRecord(Record):
  def __init__(self, line = "", split = '|||'):
    Record.__init__(self)
    self.split = split
    self.loadLine(line, split)

  def loadLine(self, line, split = '|||'):
    if line:
      fields = line.strip().split(split)
      self.src = fields[0].strip()
      self.trg = fields[1].strip()
      self.features = getFeatures(fields[2])
      listCounts = getCounts(fields[3])
      self.counts.setCounts(co = listCounts[0], src = listCounts[1], trg = listCounts[2])
      self.aligns = fields[4].strip().split()

  def getSrcSymbols(self):
    return getSymbols(self.src)
  srcSymbols = property(getSrcSymbols)

  def getSrcTerms(self):
    return getTerms(self.src)
  srcTerms = property(getSrcTerms)

  def getTrgSymbols(self):
    return getSymbols(self.trg)
  trgSymbols = property(getTrgSymbols)

  def getTrgTerms(self):
    return getTerms(self.trg)
  trgTerms = property(getTrgTerms)

  def toStr(self, s = ' ||| '):
#    strFeatures = str.join(' ', sorted(getStrFeatures(self.features)))
    strFeatures = getStrFeatures(self.features)
    strCounts   = "%s %s %s" % (self.counts.co, self.counts.src, self.counts.trg)
#    strAligns = str.join(' ', sorted(self.aligns))
    strAligns = str.join(' ', self.aligns)
    buf = str.join(s, [self.src, self.trg, strFeatures, strCounts, strAligns]) + "\n"
    return buf

  def getReversed(self):
    recRev = TravatarRecord()
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

