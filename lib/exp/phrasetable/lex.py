#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''語彙翻訳テーブル操作'''

import pprint
from collections import defaultdict

from exp.common import files
from exp.phrasetable import record

pp = pprint.PrettyPrinter()

def extractLexRec(srcFile, saveFile, RecordClass = record.MosesRecord):
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
    saveFile.write(buf)
  saveFile.close()


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
  return probs

