#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''2つのフレーズテーブルをピボット側で周辺化しし、新しく1つのフレーズテーブルを合成する．'''

import argparse
import codecs
import math
import multiprocessing
import os
import pprint
import sys
import time

from collections import defaultdict

# my exp libs
from exp.common import cache, debug, files, progress
from exp.phrasetable import findutil
from exp.phrasetable import lex, combine_lex
from exp.phrasetable.record import MosesRecord
from exp.phrasetable.reverse import reverseTable

# フレーズ対応の出力を打ち切る上限、自然対数値で指定する
#THRESHOLD = 1e-3
THRESHOLD = 0 # 打ち切りなし

# フィルタリングで残す数
NBEST = 20

# 翻訳確率の推定方法 counts/probs/counts+
# 翻訳確率の推定方法 countmin/prod/bidirect
methods = ['countmin', 'prodprob', 'prodprob', 'bidirmin', 'bidirgmean', 'bidirmax']
#METHOD = 'counts'
#METHOD = 'hybrid'
METHOD = 'countmin'

# 語彙翻訳確率の推定方法
lexMethods = ['prodweight', 'countmin', 'prodprob', 'bidirmin', 'bidirgmean', 'bidirmax', 'table', 'countmin+table', 'prodprob+table', 'bidirmin+table', 'bidirgmean+table']
LEX_METHOD = 'prodweight'

NULLS = 10**4

NOPREFILTER = False

# ピボット時にターゲットレコードの検索履歴を残す件数
#CACHESIZE = 1000
CACHESIZE = 10000

pp = pprint.PrettyPrinter()

class WorkSet:
  '''マルチプロセス処理に必要な情報をまとめたもの'''
  def __init__(self, savefile, workdir, method, RecordClass = MosesRecord, **options):
    prefix = options.get('prefix', 'phrase')
    self.multiTarget = options.get('multiTarget', False)
    self.method = method
    if method.find('multi') >= 0:
        self.multiTarget = True
        self.method = method.replace('multi','').replace('+','')
    self.nbest = NBEST
    self.outQueue = multiprocessing.Queue()
    self.pivotCount = progress.Counter(scaleup = 1000)
    self.pivotQueue = multiprocessing.Queue()
    self.savePath = savefile
    self.threshold = THRESHOLD
    self.workdir = workdir
    self.Record = RecordClass
#    if method == 'prodprob':
#        self.pivotPath = savefile
#    if method in ('counts', 'hybrid'):
#    else:
    self.pivotPath = "%s/%s_pivot" % (workdir, prefix)
    self.revPath = "%s/%s_reversed" % (workdir, prefix)
    self.trgCountPath = "%s/%s_trg" % (workdir, prefix)
    self.revTrgCountPath = "%s/%s_revtrg" % (workdir, prefix)
    self.countPath = "%s/%s_pprobs" % (workdir, prefix)
    self.tableLexPath = '%s/table.lex' % (workdir)
    self.combinedLexPath = '%s/combined.lex' % (workdir)
#    else:
#      assert False, "Invalid method"
    self.pivotProc = multiprocessing.Process( target = pivotRecPairs, args = (self,) )
    self.recordProc = multiprocessing.Process( target = writeRecordQueue, args = (self,) )

  def __del__(self):
    self.close()

  def close(self):
    if self.pivotProc.pid:
      if self.pivotProc.exitcode == None:
        self.pivotProc.terminate()
      self.pivotProc.join()
    if self.recordProc.pid:
      if self.recordProc.exitcode == None:
        self.recordProc.terminate()
      self.recordProc.join()
    self.pivotQueue.close()
    self.outQueue.close()

  def join(self):
    self.pivotProc.join()
    self.recordProc.join()

  def start(self):
    self.pivotProc.start()
    self.recordProc.start()

  def terminate(self):
    self.pivotProc.terminate()
    self.recordProc.terminate()


def updateFeatures(recPivot, recPair, method, multiTarget = False):
    '''素性の更新'''
    features = recPivot.features
    srcFeatures = recPair[0].features
    trgFeatures = recPair[1].features
    if method.find('prodprob') >= 0:
        # スコアを掛け合わせて周辺化
        if not multiTarget:
            for key in ['egfl', 'egfp', 'fgel', 'fgep']:
                features.setdefault(key, 0)
                features[key] += (srcFeatures[key] * trgFeatures[key])
        if multiTarget:
            for key in ['egfp', 'fgep']:
                features.setdefault(key, 0)
                features[key] += (srcFeatures[key] * trgFeatures[key])
            for key in ['egfl', 'egfp', 'fgel', 'fgep']:
                features['1'+key] = srcFeatures[key]
    else:
        # Lexical Weights のみ掛け合わせて周辺化
        for key in ['egfl', 'fgel']:
            features.setdefault(key, 0)
            features[key] += (srcFeatures[key] * trgFeatures[key])
    # p と w は後の方を優先する
    if 'p' in trgFeatures:
        features['p'] = trgFeatures['p']
    if multiTarget:
        if 'w' in trgFeatures:
            features['0w'] = trgFeatures['w']
        if 'w' in srcFeatures:
            features['1w'] = srcFeatures['w']
    else:
        if 'w' in trgFeatures:
            features['w'] = trgFeatures['w']


def updateCounts(recPivot, recPair, method):
    '''フレーズの出現頻度を更新'''
    counts = recPivot.counts
    features = recPivot.features
    if method == 'countmin':
        #counts.co = max(counts.co, min(recPair[0].counts.co, recPair[1].counts.co))
        #c = recPair[0].counts.co * recPair[1].counts.co
        c = min(recPair[0].counts.co, recPair[1].counts.co)
        counts.co += c
        #counts.co = counts.co + c + 2 * math.sqrt(counts.co * c)
    elif method == 'bidirmin':
        counts1 = recPair[0].counts
        counts2 = recPair[1].counts
        co1 = counts1.co * counts2.co / float(counts2.src)
        co2 = counts2.co * counts1.co / float(counts1.trg)
        counts.co += min(co1, co2)
#        if True:
#        if recPair[0].src.find('Dieu') >= 0:
#            progress.log("%s ||| %s ||| %s ||| (%s %s %s) * (%s %s %s) -> %s %s -> %s\n" % (recPair[0].src, recPair[0].trg, recPair[1].trg, counts1.co, counts1.src, counts1.trg, counts2.co, counts2.src, counts2.trg, co1, co2, min(co1,co2)))
    elif method == 'bidirgmean':
        counts1 = recPair[0].counts
        counts2 = recPair[1].counts
        co1 = counts1.co * counts2.co / float(counts2.src)
        co2 = counts2.co * counts1.co / float(counts1.trg)
        counts.co += math.sqrt(co1*co2)
#        progress.log("%s ||| %s ||| %s ||| (%s %s %s) * (%s %s %s) -> %s %s -> %s\n" % (recPair[0].src, recPair[0].trg, recPair[1].trg, counts1.co, counts1.src, counts1.trg, counts2.co, counts2.src, counts2.trg, co1, co2, math.sqrt(co1*co2)))
    elif method == 'bidirmax':
        counts1 = recPair[0].counts
        counts2 = recPair[1].counts
        co1 = counts1.co * counts2.co / float(counts2.src)
        co2 = counts2.co * counts1.co / float(counts1.trg)
        counts.co += max(co1, co2)
    elif method == 'prodprob':
        counts.src = recPair[0].counts.src
        counts.co  = counts.src * features['egfp']
        counts.trg = counts.co / features['fgep']
    elif method == 'multi':
        c = min(recPair[0].counts.co, recPair[1].counts.co)
        counts.co += c
    else:
        assert False, "Invalid method"


def mergeAligns(recPivot, recPair):
  '''アラインメントのマージを試みる'''
  if recPivot.aligns:
    return
  alignSet = set()
  alignMapSrcPvt = recPair[0].alignMap
  alignMapPvtTrg = recPair[1].alignMap
  for srcIndex, pvtIndices in alignMapSrcPvt.items():
    for pvtIndex in pvtIndices:
      for trgIndex in alignMapPvtTrg.get(pvtIndex, []):
        align = '%d-%d' % (srcIndex, trgIndex)
        alignSet.add(align)
  recPivot.aligns = sorted(alignSet)


def filterByCountRatioToMax(records, div = 100):
  coMax = 0
  for rec in flattenRecords(records):
    coMax = max(coMax, rec.counts.co)
  if isinstance(records, list):
    newRecords = []
    for rec in records:
      if rec.counts.co >= coMax / float(div):
        newRecords.append( rec )
    records = newRecords
  elif isinstance(records, dict):
    newRecords = {}
    for key, rec in records.items():
      if rec.counts.co >= coMax / float(div):
        newRecords[key] = rec
    records = newRecords
  return records


def calcPhraseTransProbsByCounts(records):
    '''フレーズの出現回数から順方向のフレーズ翻訳確率を計算'''
    srcCount = calcSrcCount(records)
    for rec in records.values():
    #for rec in flattenRecords(records):
        counts = rec.counts
        counts.src = srcCount
        rec.features['egfp'] = counts.co / float(srcCount)



def calcPhraseTransProbsOnTable(tablePath, savePath, **options):
    '''共起回数の推定されたテーブルを元にフレーズ翻訳確率を計算'''
    method = options.get('method', METHOD)
    RecordClass = options.get('RecordClass', MosesRecord)

    tableFile = files.open(tablePath, "r")
    saveFile  = files.open(savePath, "w")
    records = {}
    lastSrc = ''
    for line in tableFile:
        rec = RecordClass(line)
        key = "%s ||| %s |||" % (rec.src, rec.trg)
        if rec.src != lastSrc and records:
            calcPhraseTransProbsByCounts(records)
            writeRecords(saveFile, records)
            records = {}
        records[key] = rec
        lastSrc = rec.src
    if records:
        calcPhraseTransProbsByCounts(records)
        writeRecords(saveFile, records)
    saveFile.close()
    tableFile.close()


def calcSrcCount(records):
  '''共起回数から原言語フレーズの出現回数を求める'''
  total = 0
  for rec in flattenRecords(records):
    total += rec.counts.co
  return total

def updateWordPairCounts(lexCounts, records):
    '''単単語のフレーズ対応を見つけ出して、単語対応のカウントを更新する'''
    if len(records) > 0:
        srcSymbols = records.values()[0].srcSymbols
        if len(srcSymbols) == 1:
           for rec in records.values():
               trgSymbols = rec.trgSymbols
               if len(trgSymbols) == 1:
                   lexCounts.addPair(srcSymbols[0], trgSymbols[0], rec.counts.co)
        lexCounts.filterNBestBySrc(srcWord = srcSymbols[0])

def flattenRecords(records, sort = False):
  '''レコード群が辞書であればリストにして返す'''
  if type(records) == dict:
    if sort:
      recordList = []
      for key in sorted(records.keys()):
        recordList.append(records[key])
      return recordList
    else:
      return records.values()
  elif type(records) == list:
    if sort:
      return sorted(records)
    else:
      return records
  else:
    assert False, "Invalid records"


def pivotRecPairs(workset):
  '''ピボット側で共通するレコードの組を統合する

  ピボット対象のレコード組のリストを pivotQueue で受け取り、処理したデータを outQueue で渡す

  workset.method が "prodprob" の場合、
  翻訳確率を掛け合わせて周辺化によって新しい翻訳確率を推定する

  それ以外の場合、
  共起回数を推定することで翻訳確率の推定を行う
  '''

  lexCounts = lex.PairCounter()

  while True:
    # 処理すべきレコード配列を取得
    rows = workset.pivotQueue.get()
    if rows == None:
        # None を受け取ったらプロセス終了
        break
    records = {}
    if workset.multiTarget:
        multiRecords = {}
    for recPair in rows:
        trgKey = recPair[1].trg + ' |||'
        if workset.multiTarget:
            strMultiTrg = intern(recPair[1].trg + ' |COL| ' + recPair[0].trg)
            multiKey = strMultiTrg + ' |||'
        if not trgKey in records:
            # 対象言語の訳出のレコードがまだ無いので作る
            recPivot = workset.Record()
            recPivot.src = recPair[0].src
            recPivot.trg = recPair[1].trg
            records[trgKey] = recPivot
        recPivot = records[trgKey]
        if workset.multiTarget:
            recMulti = workset.Record()
            recMulti.src = recPair[0].src
            recMulti.trg = strMultiTrg
            multiRecords[multiKey] = recMulti
        # 素性の推定、更新
        updateFeatures(recPivot, recPair, workset.method)
        # 句対応のカウントを更新
        updateCounts(recPivot, recPair, workset.method)
        # アラインメントのマージ
        mergeAligns(recPivot, recPair)
        if workset.multiTarget:
            updateFeatures(recMulti, recPair, workset.method, multiTarget = True)
    # この時点で1つの原言語フレーズと、対応する目的言語フレーズが確定する
    if workset.multiTarget:
        # マルチターゲットレコードに通常のピボットレコードのスコアを適用
        for multiKey, recMulti in multiRecords.items():
            trgPair = recMulti.trg.split(' |COL| ')
            recPivot = records[trgPair[0]+' |||']
            for featureKey in ['egfl', 'egfp', 'fgel', 'fgep']:
                recMulti.features['0'+featureKey] = recPivot.features[featureKey]
    if workset.method != 'prodprob':
        # 単単語のフレーズ対応を見つけ出して、単語対応をカウントする
        updateWordPairCounts(lexCounts, records)
        # 共起回数のn-bestでフィルタリングする
        if not NOPREFILTER:
            if workset.nbest > 0:
                if len(records) > workset.nbest:
                    scores = []
                    for key, rec in records.items():
                        scores.append( (rec.counts.co, key) )
                    scores.sort(reverse = True)
                    bestRecords = {}
                    for _, key in scores[:workset.nbest]:
                        bestRecords[key] = records[key]
                    records = bestRecords
        # 順方向のフレーズ翻訳確率を求める
        calcPhraseTransProbsByCounts(records)
    # threshold が設定されている場合、しきい値以下の翻訳確率を持つレコードは無視
    if workset.threshold < 0:
        # 非常に小さな翻訳確率のフレーズは無視する
        ignoring = []
        for key, rec in records.items():
            if rec[0]['fgep'] < workset.threshold and rec[0]['egfp'] < workset.threshold:
                ignoring.append(pair)
        for key in ignoring:
            del records[key]
    if workset.multiTarget:
        records = multiRecords
    # n-best が設定されている場合は順方向の翻訳確率でソートしてフィルタリング
    if workset.nbest > 0:
        if len(records) > workset.nbest:
            scores = []
            for key, rec in records.items():
                if workset.multiTarget:
                    scores.append( (rec.features['1egfp'],key) )
                else:
                    scores.append( (rec.features['egfp'],key) )
            scores.sort(reverse = True)
            bestRecords = {}
            for _, key in scores[:workset.nbest]:
                bestRecords[key] = records[key]
            records = bestRecords
    # レコードをキューに追加して、別プロセスに書き込んでもらう
    if records:
      workset.pivotCount.add( len(records) )
      for trgKey in sorted(records.keys()):
        rec = records[trgKey]
        workset.outQueue.put( rec )
  # while ループを抜けた
  # writeRecords のプロセスも終わらせる
  workset.outQueue.put(None)
  if workset.method != 'prodprob':
      lexCounts.filterNBestByTrg()
      lex.saveWordPairCounts(workset.tableLexPath, lexCounts)


def writeRecords(fileObj, records):
#  for rec in flattenRecords(records):
  for rec in flattenRecords(records, sort = True):
      fileObj.write( rec.toStr() )


def writeRecordQueue(workset):
  '''キューに溜まったピボット済みのレコードをファイルに書き出す'''
  pivotFile = files.open(workset.pivotPath, 'w')
  while True:
    rec = workset.outQueue.get()
    if rec == None:
      # Mone を受け取ったらループ終了
      break
    pivotFile.write( rec.toStr() )
  pivotFile.close()


class PivotFinder:
  def __init__(self, table1, table2, index1, index2, RecordClass = MosesRecord):
    self.srcFile = files.open(table1, 'r')
    self.trgFile = files.open(table2, 'r')
    self.srcIndices = findutil.loadIndices(index1)
    self.trgIndices = findutil.loadIndices(index2)
    self.srcCount = progress.Counter(scaleup = 1000)
    self.rows = []
    self.rowsCache = cache.Cache(size = CACHESIZE)
    self.Record = RecordClass

  def getRow(self):
    if self.rows == None:
      return None
    while len(self.rows) == 0:
      line = self.srcFile.readline()
      self.srcCount.add()
      if not line:
        self.rows = None
        return None
      self.makePivot(line)
    return self.rows.pop(0)

  def makePivot(self, srcLine):
    recSrc = self.Record(srcLine)
    pivotPhrase = recSrc.trg

    if pivotPhrase in self.rowsCache:
      trgLines = self.rowsCache[pivotPhrase]
      self.rowsCache.use(pivotPhrase)
    else:
      trgLines = findutil.searchIndexed(self.trgFile, self.trgIndices, pivotPhrase)
      self.rowsCache[pivotPhrase] = trgLines
    for trgLine in trgLines:
      recTrg = self.Record(trgLine)
      self.rows.append( [recSrc, recTrg] )


def calcLexWeight(rec, lexCounts, reverse = False):
#  minProb = 10 ** -2
  lexWeight = 1
#  alignMapRev = rec.alignMapRev
  if not reverse:
    minProb  = 1 / float(lexCounts.trgCounts["NULL"])
    # 順方向の場合は逆向きのアラインメントマップを使う
    alignMap = rec.alignMapRev
    srcTerms = rec.srcTerms
    trgTerms = rec.trgTerms
  else:
    minProb = 1 / float(lexCounts.srcCounts["NULL"])
    alignMap = rec.alignMap
    srcTerms = rec.trgTerms
    trgTerms = rec.srcTerms
  for trgIndex in range(len(trgTerms)):
    trgTerm = trgTerms[trgIndex]
    if trgIndex in alignMap:
      trgSumProb = 0
      srcIndices = alignMap[trgIndex]
      for srcIndex in srcIndices:
        srcTerm = srcTerms[srcIndex]
#        pair = (srcTerm, trgTerm)
#        lexProb = lexProbs.get(pair, minProb)
        if not reverse:
          lexProb = lexCounts.calcLexProb(srcTerm, trgTerm)
        else:
          lexProb = lexCounts.calcLexProbRev(trgTerm, srcTerm)
        trgSumProb += lexProb
      lexWeight *= (trgSumProb / len(srcIndices))
    else:
#      lexWeight *= minProb
      if not reverse:
        lexWeight *= lexCounts.calcLexProb("NULL", trgTerm)
      else:
        lexWeight *= lexCounts.calcLexProb(trgTerm, "NULL")
  return lexWeight

def calcLexWeights(tablePath, lexCounts, savePath, RecordClass = MosesRecord):
  tableFile = files.open(tablePath, 'r')
  saveFile  = files.open(savePath, 'w')
  for line in tableFile:
    rec = RecordClass(line)
#    rec.features['egfl'] = calcLexWeight(rec, lexCounts)
    rec.features['egfl'] = calcLexWeight(rec, lexCounts, reverse = False)
    rec.features['fgel'] = calcLexWeight(rec, lexCounts, reverse = True)
    saveFile.write( rec.toStr() )
  saveFile.close()
  tableFile.close()


def pivot(table1, table2, savefile="phrase-table.gz", workdir=".", **options):
  # recSymbols -> recSymbols -> recSymbols の形の訳出を探す
  try:
    # オプション初期値の設定
    RecordClass = options.get('RecordClass', MosesRecord)
    prefix = options.get('prefix', 'phrase')
    threshold = options.get('threshold', THRESHOLD)
    alignLexPath   = options.get('alignlex', None)
    nbest     = options.get('nbest', NBEST)
    method    = options.get('method', METHOD)
    lexMethod = options.get('lexmethod', LEX_METHOD)
    numNulls  = options.get('nulls', NULLS)
#    multiTarget = options.get('multitarget', False)

    if lexMethod not in ('prodweight', 'table'):
        if alignLexPath == None:
            debug.log(lexMethod)
            assert False, "aligned lexfile is not given"

    # 作業ディレクトリの作成
    workdir = workdir + '/pivot'
    files.mkdir(workdir)
    # テーブル1の展開
    if files.isGzipped(table1):
        srcWorkTable = "%s/%s_src-pvt" % (workdir, prefix)
        progress.log("table copying into: %s\n" % srcWorkTable)
        files.autoCat(table1, srcWorkTable)
    else:
        srcWorkTable = table1
    # テーブル2の展開
    if files.isGzipped(table2):
        trgWorkTable = "%s/%s_pvt-trg" % (workdir, prefix)
        progress.log("table copying into: %s\n" % trgWorkTable)
        files.autoCat(table2, trgWorkTable)
    else:
        trgWorkTable = table2
    # インデックス1の作成
    srcIndex = srcWorkTable + '.index'
    progress.log("making index: %s\n" % srcIndex)
    findutil.saveIndices(srcWorkTable, srcIndex)
    # インデックス2の作成
    trgIndex = trgWorkTable + '.index'
    progress.log("making index: %s\n" % trgIndex)
    findutil.saveIndices(trgWorkTable, trgIndex)
    # ワークセットの作成
    workset = WorkSet(savefile, workdir, method, RecordClass = RecordClass, prefix = prefix)
    workset.threshold = threshold
    workset.nbest = nbest
    # ワークセットの起動
    workset.start()
    # ピボットで対応するレコードを網羅する
    finder = PivotFinder(srcWorkTable, trgWorkTable, srcIndex, trgIndex, RecordClass = RecordClass)
    currPhrase = ''
    rows = []
    rowCount = 0
    progress.log("beginning pivot\n")
    while True:
      if workset.pivotQueue.qsize() > 2000:
        time.sleep(1)
      row = finder.getRow()
      if not row:
        break
      rowCount += 1
      srcPhrase = row[0].src
      if currPhrase != srcPhrase and rows:
        # 新しい原言語フレーズが出てきたので、ここまでのデータを開いてるプロセスに処理してもらう
        workset.pivotQueue.put(rows)
        rows = []
        currPhrase = srcPhrase
        #debug.log(workset.record_queue.qsize())
      rows.append(row)
      if finder.srcCount.shouldPrint():
        finder.srcCount.update()
        numSrcRecords = finder.srcCount.count
        ratio = 100.0 * numSrcRecords / len(finder.srcIndices)
        progress.log("source: %d (%3.2f%%), processed: %d, last %s: %s" %
                     (numSrcRecords, ratio, rowCount, prefix, srcPhrase) )
    # while ループを抜けた
    # 最後のデータ処理
    workset.pivotQueue.put(rows)
    workset.pivotQueue.put(None)
    # 書き出しプロセスの正常終了待ち
    workset.join()
    progress.log("source: %d (100%%), processed: %d, pivot %d  \n" %
                 (finder.srcCount.count, rowCount, workset.pivotCount.count) )
    # ワークセットを片付ける
    workset.close()

    # 必要な単語対カウントファイルを読み込む
    if lexMethod != 'prodweight':
        if lexMethod.find('table') >= 0:
            if lexMethod.find('+') >= 0:
                progress.log("combining lex counts into: %s\n" % (workset.combinedLexPath))
                combine_lex.combine_lex(alignLexPath, workset.tableLexPath, workset.combinedLexPath)
                progress.log("loading combined word trans probabilities\n")
                lexCounts = lex.loadWordPairCounts(workset.combinedLexPath)
            else:
                progress.log("loading table lex: %s\n", workset.tableLexPath)
                lexCounts = lex.loadWordPairCounts(workset.tableLexPath)
                lexCounts.srcCounts["NULL"] = numNulls
                lexCounts.trgCounts["NULL"] = numNulls
        else:
            progress.log("loading aligned lex: %s\n", alignLexPath)
            lexCounts = lex.loadWordPairCounts(alignLexPath)
#    if workset.method == 'countmin':
    if method in ['countmin', 'bidirmin', 'bidirgmean', 'bidirmax']:
  #      # 単語単位の翻訳確率をロードする
  #      #progress.log("loading word trans probabilities\n")
  #      #lexCounts = lex.loadWordPairCounts(lexPath)
  #      progress.log("combining lex counts into: %s\n" % (workset.combinedLexPath))
  #      combine_lex.combine_lex(lexPath, workset.tableLexPath, workset.combinedLexPath)
  #      progress.log("loading combined word trans probabilities\n")
  #      lexCounts = lex.loadWordPairCounts(workset.combinedLexPath)
        # テーブルを逆転させる
        progress.log("reversing %s table into: %s\n" % (prefix, workset.revPath) )
        reverseTable(workset.pivotPath, workset.revPath, RecordClass)
        progress.log("reversed %s table\n" % (prefix))
        # 逆転したテーブルで逆方向のフレーズ翻訳確率を求める
        progress.log("calculating reversed phrase trans probs into: %s\n" % (workset.trgCountPath))
        calcPhraseTransProbsOnTable(workset.revPath, workset.trgCountPath, nbest = workset.nbest, RecordClass = RecordClass)
        progress.log("calculated reversed phrase trans probs\n")
        # 再度テーブルを反転して元に戻す
#        progress.log("reversing %s table into: %s\n" % (prefix,workset.revTrgCountPath))
        progress.log("reversing %s table into: %s\n" % (prefix,workset.countPath))
#        reverseTable(workset.trgCountPath, workset.revTrgCountPath, RecordClass)
        reverseTable(workset.trgCountPath, workset.countPath, RecordClass)
        progress.log("reversed %s table\n" % (prefix))
#        # 順方向の翻訳確率を求める
#        progress.log("calculating phrase trans probs into: %s\n" % (workset.countPath))
#        calcPhraseTransProbsOnTable(workset.revTrgCountPath, workset.countPath, nbest = 0, RecordClass = RecordClass)
#        progress.log("calculated phrase trans probs\n")
        if lexMethod != 'prodweight':
            # 語彙化翻訳確率を求める
            progress.log("calculating lex weights into: %s\n" % workset.savePath)
            calcLexWeights(workset.countPath, lexCounts, workset.savePath, RecordClass)
            progress.log("calculated lex weights\n")
        else:
            progress.log("gzipping into: %s\n" % workset.savePath)
            files.autoCat(workset.countPath, workset.savePath)
#    elif method == 'prodprob':
    elif method.find('prodprob') >= 0:
        if lexMethod != 'prodweight':
            # 語彙化翻訳確率を求める
            progress.log("calculating lex weights into: %s\n" % workset.savePath)
            calcLexWeights(workset.pivotPath, lexCounts, workset.savePath, RecordClass)
            progress.log("calculated lex weights\n")
        else:
            progress.log("gzipping into: %s\n" % workset.savePath)
            files.autoCat(workset.pivotPath, workset.savePath)
    elif method == 'multi':
            progress.log("gzipping into: %s\n" % workset.savePath)
            files.autoCat(workset.pivotPath, workset.savePath)
    else:
        assert False, "Invalid method: %s" % method
  except KeyboardInterrupt:
    # 例外発生、全てのワーカープロセスを停止させる
    print('')
    print('Caught KeyboardInterrupt, terminating all the worker processes')
    workset.close()
    sys.exit(1)

def main():
  parser = argparse.ArgumentParser(description = 'load 2 phrase tables and pivot into one moses phrase table')
  parser.add_argument('table1', help = 'phrase table 1')
  parser.add_argument('table2', help = 'phrase table 2')
  parser.add_argument('savefile', help = 'path for saving moses phrase table file')
  parser.add_argument('--threshold', help = 'threshold for ignoring the phrase translation probability (real number)', type=float, default=THRESHOLD)
  parser.add_argument('--nbest', help = 'best n scores for phrase pair filtering (default = 20)', type=int, default=NBEST)
  parser.add_argument('--method', help = 'triangulation method', choices=methods, default=METHOD)
  parser.add_argument('--lexmethod', help = 'lexical triangulation method', choices=lexMethods, default=LEX_METHOD)
  parser.add_argument('--workdir', help = 'working directory', default='.')
  parser.add_argument('--alignlex', help = 'word pair counts file', default=None)
  parser.add_argument('--nulls', help = 'number of NULLs (lines) for table lex', type = int, default=NULLS)
  parser.add_argument('--noprefilter', help = 'No pre-filtering', type = bool, default=False)
  args = vars(parser.parse_args())

  if args['noprefilter']:
      NOPREFILTER = args['noprefilter']

  pivot(**args)

if __name__ == '__main__':
  main()

