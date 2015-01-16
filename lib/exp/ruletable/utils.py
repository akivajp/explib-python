#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''ルールテーブルの操作関数群'''

import codecs
import pprint
import subprocess

from collections import defaultdict

from exp.common import files
from exp.ruletable import record

from exp.ruletable.record import TravatarRecord

pp = pprint.PrettyPrinter()

def extractLexRec(srcFile, saveFile):
  if type(srcFile) == str:
    srcFile = files.open(srcFile)
  if type(saveFile) == str:
    saveFile = files.open(saveFile, 'w')
  srcCount = defaultdict(lambda: 0)
  trgCount = defaultdict(lambda: 0)
  coCount  = defaultdict(lambda: 0)
  for line in srcFile:
    rec = record.TravatarRecord(line)
    srcSymbols = rec.srcSymbols
    trgSymbols = rec.trgSymbols
    if len(srcSymbols) == 1 and len(trgSymbols) == 1:
      src = srcSymbols[0]
      trg = trgSymbols[0]
      srcCount[src] += rec.counts.co
      trgCount[trg] += rec.counts.co
      coCount[(src,trg)] += rec.counts.co
  for pair in sorted(coCount.keys()):
    (src,trg) = pair
    egfl = coCount[pair] / float(srcCount[src])
    fgel = coCount[pair] / float(trgCount[trg])
    buf = "%s %s %s %s\n" % (src, trg, egfl, fgel)
    #buf = "%s %s %s %s %s %s %s\n" % (src, trg, egfl, fgel, coCount[pair], srcCount[src], trgCount[trg])
    saveFile.write(buf)
  saveFile.close()

#def getAlignMaps(field):
#  alignMap = {}
#  revMap = {}
#  for align in field.split():
#    (s, t) = map(int, align.split('-'))
#    alignMap.setdefault(s, []).append(t)
#    revMap.setdefault(t, []).append(s)
#  return alignMap, revMap

#def getCounts(field):
#  return map(getNumber, field.split())

#def getFeatures(field):
#  features = {}
#  for strKeyVal in field.split():
#    (key, val) = strKeyVal.split('=')
#    features[key] = getNumber(val)
#  return features

#def getKeyValList(dic):
#  '''辞書を、'key=val' という文字列で表したリストに変換する'''
#  l = []
#  for key, val in dic.items():
#    l.append( "%s=%s" % (key, val) )
#  return l

#def getNumber(strNum):
#  numFloat = float(strNum)
#  numInt = int(numFloat)
#  if numFloat == numInt:
#    return numInt
#  else:
#    return numFloat

#def getRecStr(src, trg, features, counts, aligns):
#  if type(features) == dict:
#    features = str.join(' ', sorted(getKeyValList(features)))
#  if type(counts) == list:
#    counts = str.join(' ', map(str,counts))
#  if type(aligns) == list:
#    aligns = str.join(' ', sorted(aligns))
#  buf  = src + ' ||| '
#  buf += trg + ' ||| '
#  buf += features + ' ||| '
#  buf += counts + ' ||| '
#  buf += aligns
#  buf += "\n"
#  return buf

#def getTerms(rule):
#  terms = []
#  for elem in rule.split():
#    if elem == "@":
#      break
#    elif elem[0] == '"':
#      terms.append(elem)
#  return terms


def loadWordProbs(srcFile, reverse = False):
  if type(srcFile) == str:
    srcFile = files.open(srcFile)
  probs = {}
  for line in srcFile:
    fields = line.strip().split()
    src = fields[0]
    trg = fields[1]
    if not reverse:
      probs[(src, trg)] = float(fields[2])
    else:
      probs[(trg, src)] = float(fields[3])
#    probs[(src, trg)] = [float(fields[2]), float(fields[3])]
  return probs


#def reverseAligns(aligns):
#  revAligns = []
#  for align in aligns:
#    (srcIndex, trgIndex) = align.split('-')
#    revAligns.append(trgIndex + '-' + srcIndex)
#  return revAligns

#def reverseFeatures(features, target):
#  revFeatures = {}
#  if 'egfp' in features:
#    revFeatures['fgep'] = features['egfp']
#  if 'egfl' in features:
#    revFeatures['fgel'] = features['egfl']
#  if 'fgep' in features:
#    revFeatures['egfp'] = features['fgep']
#  if 'fgel' in features:
#    revFeatures['egfl'] = features['fgel']
#  if 'p' in features:
#    revFeatures['p'] = features['p']
#  w = 0
#  for word in target.split():
#    if word == "@":
#      break
#    if word[0] == '"':
#      w += 1
#  revFeatures['w'] = str(w)
#  return revFeatures

def reverseTable(srcFile, saveFile):
  if type(srcFile) == str:
    srcFile = files.open(srcFile)
  if type(saveFile) == str:
    if files.getExt(saveFile) == '.gz':
      saveFile = open(saveFile, 'w')
      pipeGzip = subprocess.Popen(['gzip'], stdin=subprocess.PIPE, stdout=saveFile)
      saveFile = pipeGzip.stdin
    else:
      saveFile = open(saveFile, 'w')
  pipeSort = subprocess.Popen(['sort'], env={"LC_ALL":"C"}, stdin=subprocess.PIPE, stdout=saveFile)
  inputSort = codecs.getwriter('utf-8')(pipeSort.stdin)
  for line in srcFile:
#    fields = line.strip().split('|||')
#    src = fields[0].strip()
#    trg = fields[1].strip()
#    features = getFeatures(fields[2])
#    counts = getCounts(fields[3])
#    aligns = fields[4].split()
#    revFeatures = reverseFeatures(features, src)
#    revCounts = [counts[0], counts[2], counts[1]]
#    revAligns = reverseAligns(aligns)
#    buf = getRecStr(trg, src, revFeatures, revCounts, revAligns)
    rec = TravatarRecord(line)
#    inputSort.write( buf )
    inputSort.write( rec.getReversed().toStr() )
  pipeSort.stdin.close()
  pipeSort.communicate()
  saveFile.close()

