#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''語彙翻訳テーブル操作'''

import codecs
import math
import pprint
import sys
from collections import defaultdict

from exp.common import cache
from exp.common import files
from exp.common import number
from exp.common import progress
from exp.phrasetable import record

stdout = codecs.getwriter('utf-8')(sys.stdout)
pp = pprint.PrettyPrinter()

IGNORE = 20
#IGNORE = 10
#IGNORE = 0

# 整数近似判定に使う
MARGIN = 0.0001

# countmin/prodprob
METHOD = 'countmin'

class PairCounter(object):
  def __init__(self):
    self.srcCounts  = defaultdict(lambda: 0)
    self.trgCounts  = defaultdict(lambda: 0)
    self.pairCounts  = defaultdict(lambda: 0)
    self.srcAligned  = defaultdict(lambda: set())
    self.trgAligned  = defaultdict(lambda: set())

  def addSrc(self, word, count = 1):
    self.srcCounts[word] += count
  def setSrc(self, word, count):
    self.srcCounts[word] = count

  def addTrg(self, word, count = 1):
    self.trgCounts[word] += count
  def setTrg(self, word, count):
    self.trgCounts[word] = count

  def addPair(self, srcWord, trgWord, count = 1):
    self.pairCounts[(srcWord, trgWord)] += count
    self.srcAligned[trgWord].add(srcWord)
    self.trgAligned[srcWord].add(trgWord)

  def addNull(self, count = 1):
    self.addSrc( intern("NULL") )
#    self.addSrc( cache.intern("NULL") )
    self.addTrg( intern("NULL") )
#    self.addTrg( cache.intern("NULL") )

  def calcLexProb(self, srcWord, trgWord):
    coCount = self.pairCounts[(srcWord,trgWord)]
    if coCount == 0:
      return 1 / float(self.trgCounts["NULL"])
    else:
      return coCount / float(self.srcCounts[srcWord])

  def calcLexProbRev(self, srcWord, trgWord):
    coCount = self.pairCounts[(srcWord,trgWord)]
    if coCount == 0:
      return 1 / float(self.srcCounts["NULL"])
    else:
      return coCount / float(self.trgCounts[trgWord])


def calcWordPairCountsByAligns(srcTextPath, trgTextPath, alignPath):
    srcTextFile = files.open(srcTextPath, 'r')
    trgTextFile = files.open(trgTextPath, 'r')
    alignFile = files.open(alignPath, 'r')
    pairCounter = PairCounter()
    while True:
        srcLine = srcTextFile.readline()
        trgLine = trgTextFile.readline()
        alignLine = alignFile.readline()
        if srcLine == "":
          break
        srcWords = srcLine.strip().split(' ')
        trgWords = trgLine.strip().split(' ')
        alignList = alignLine.strip().split(' ')
        pairCounter.addNull()
        for word in srcWords:
          pairCounter.addSrc(word)
        for word in trgWords:
          pairCounter.addTrg(word)
        srcAlignedIndices = set()
        trgAlignedIndices = set()
        for align in alignList:
          (srcIndex, trgIndex) = map(int, align.split('-'))
          srcWord = srcWords[srcIndex]
          trgWord = trgWords[trgIndex]
          pairCounter.addPair(srcWord, trgWord)
          srcAlignedIndices.add( srcIndex )
          trgAlignedIndices.add( trgIndex )
        for i, srcWord in enumerate(srcWords):
          if not i in srcAlignedIndices:
            pairCounter.addPair(srcWord, "NULL")
        for i, trgWord in enumerate(trgWords):
          if not i in trgAlignedIndices:
            pairCounter.addPair("NULL", trgWord)
    return pairCounter


def saveWordPairCounts(savePath, pairCounter):
  saveFile = files.open(savePath, 'w')
  for pair in sorted(pairCounter.pairCounts.keys()):
    srcWord = pair[0]
    trgWord = pair[1]
    srcCount = number.toNumber(pairCounter.srcCounts[srcWord], MARGIN)
    trgCount = number.toNumber(pairCounter.trgCounts[trgWord], MARGIN)
    pairCount = number.toNumber(pairCounter.pairCounts[pair], MARGIN)
    buf = "%s %s %s %s %s\n" % (srcWord, trgWord, pairCount, srcCount, trgCount)
    saveFile.write( buf )
  saveFile.close()


def loadWordPairCounts(lexPath):
  lexFile = files.open(lexPath, 'r')
  pairCounter = PairCounter()
  for line in lexFile:
    fields = line.split()
    srcWord = intern( fields[0] )
#    srcWord = cache.intern( fields[0] )
    trgWord = intern( fields[1] )
#    trgWord = cache.intern( fields[1] )
#    pairCounter.addPair(srcWord, trgWord, int(fields[2]))
    pairCounter.addPair(srcWord, trgWord, number.toNumber(fields[2]))
#    pairCounter.setSrc(srcWord, int(fields[3]))
    pairCounter.setSrc(srcWord, number.toNumber(fields[3]))
#    pairCounter.setTrg(trgWord, int(fields[4]))
    pairCounter.setTrg(trgWord, number.toNumber(fields[4]))
  return pairCounter


#def pivotWordPairCounts(cntSrcPvt, cntPvtTrg, nbest):
def pivotWordPairCounts(cntSrcPvt, cntPvtTrg, **options):
    nbest  = options.get('nbest', 0)
    method = options.get('method', METHOD)
    cntSrcTrg = PairCounter()
    for srcWord in cntSrcPvt.srcCounts.keys():
        tmpCount = defaultdict(lambda: 0)
        for pvtWord in cntSrcPvt.trgAligned[srcWord]:
            if pvtWord == "NULL":
                # NULL はピボットになれない
                pass
            else:
                for trgWord in cntPvtTrg.trgAligned[pvtWord]:
                    coCount1 = cntSrcPvt.pairCounts[(srcWord,pvtWord)]
                    coCount2 = cntPvtTrg.pairCounts[(pvtWord,trgWord)]
                    srcCount1 = cntSrcPvt.srcCounts[srcWord]
                    srcCount2 = cntPvtTrg.srcCounts[pvtWord]
                    trgCount1 = cntSrcPvt.trgCounts[pvtWord]
                    trgCount2 = cntPvtTrg.trgCounts[trgWord]
                    if method == 'countmin':
                        if srcWord == "NULL" and trgWord == "NULL":
                            # NULL と NULL はアラインメントされない
                            pass
                        else:
                            count1 = cntSrcPvt.pairCounts[(srcWord,pvtWord)]
                            count2 = cntPvtTrg.pairCounts[(pvtWord,trgWord)]
                            minCount = min(count1, count2)
                            tmpCount[trgWord] += minCount
                    elif method == 'prodprob':
                        probSrcPvt = coCount1 / float(srcCount1)
                        probPvtTrg = coCount2 / float(srcCount2)
                        probSrcTrg = probSrcPvt * probPvtTrg
                        coCount = srcCount1 * probSrcTrg
                        tmpCount[trgWord] += coCount
#                            progress.log("%s %s (%s %s %s) * (%s %s %s) -> %s\n" % (srcWord, trgWord, coCount1, srcCount1, trgCount1, coCount2, srcCount2, trgCount2, coCount))
                    elif method == 'bidirmin':
                        co1 = coCount1 * coCount2 / float(srcCount2)
                        co2 = coCount2 * coCount1 / float(trgCount1)
                        tmpCount[trgWord] += min(co1, co2)
                    elif method == 'bidirgmean':
                        co1 = coCount1 * coCount2 / float(srcCount2)
                        co2 = coCount2 * coCount1 / float(trgCount1)
                        tmpCount[trgWord] += math.sqrt(co1*co2)
                    else:
                        assert False, "Invalid method: %s" % method
        if srcWord != "NULL":
#            if method == 'count':
#                # maxCount / IGNORE以下は無視する
#                if IGNORE > 0:
#                    maxCount = 0
#                    for trgWord, count in tmpCount.items():
#                        maxCount = max(maxCount, count)
#                    for trgWord, count in tmpCount.items():
#                        if count < maxCount / float(IGNORE):
#                            if trgWord != "NULL":
#                                del tmpCount[trgWord]
            # n-best だけ残す
            if nbest > 0:
                scores = []
                for trgWord, count in tmpCount.items():
                    scores.append( (count, trgWord) )
                scores.sort(reverse = True)
                for _, trgWord in scores[nbest:]:
                    del tmpCount[trgWord]
        # 共起回数を確定
        for trgWord, count in tmpCount.items():
            cntSrcTrg.addPair(srcWord, trgWord, count)
    # 逆向きに走査してフィルタリング
    for trgWord in cntPvtTrg.trgCounts.keys():
        if trgWord != "NULL":
#            if method == 'count':
#                # maxCount / IGNORE以下は無視する
#                if IGNORE > 0:
#                    maxCount = 0
#                    for srcWord in cntSrcTrg.srcAligned[trgWord]:
#                        maxCount = max(maxCount, cntSrcTrg.pairCounts[(srcWord,trgWord)])
#                    for srcWord in cntSrcTrg.srcAligned[trgWord]:
#                        count = cntSrcTrg.pairCounts[(srcWord,trgWord)]
#                        if count < maxCount / float(IGNORE):
#                            if srcWord != "NULL":
#                                del cntSrcTrg.pairCounts[(srcWord, trgWord)]
            if nbest > 0:
                scores = []
                for srcWord in cntSrcTrg.srcAligned[trgWord]:
                    scores.append( (cntSrcTrg.pairCounts[(srcWord,trgWord)], srcWord) )
                scores.sort(reverse = True)
                for _, srcWord in scores[nbest:]:
                    del cntSrcTrg.pairCounts[(srcWord, trgWord)]
    for pair, count in cntSrcTrg.pairCounts.items():
        if count == 0:
            del cntSrcTrg.pairCounts[pair]
        else:
            cntSrcTrg.addSrc( pair[0], count )
            cntSrcTrg.addTrg( pair[1], count )
    return cntSrcTrg


def combineWordPairCounts(lexCounts1, lexCounts2):
  lexCounts = PairCounter()
  for pair, count in lexCounts1.pairCounts.items():
    lexCounts.addPair(pair[0], pair[1], count)
    lexCounts.addSrc(pair[0], count)
    lexCounts.addTrg(pair[1], count)
  for pair, count in lexCounts2.pairCounts.items():
    lexCounts.addPair(pair[0], pair[1], count)
    lexCounts.addSrc(pair[0], count)
    lexCounts.addTrg(pair[1], count)
  return lexCounts


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

