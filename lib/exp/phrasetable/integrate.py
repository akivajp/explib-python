#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''同じ言語対を扱う2つのフレーズテーブルを新しく1つのフレーズテーブルに合成する．'''

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
from exp.phrasetable import lex
from exp.phrasetable import triangulate
from exp.phrasetable.record import MosesRecord
from exp.phrasetable.record import RecordReader
from exp.phrasetable.reverse import reverseTable

# フィルタリングで残す数
NBEST = 20

# 翻訳確率の推定方法 counts/probs
METHOD = 'counts'

pp = pprint.PrettyPrinter()


def integrateTablePair(tablePath1, tablePath2, savePath, **options):
  RecordClass = options.get('RecordClass', MosesRecord)

  recReader1 = RecordReader(tablePath1, **options)
  recReader2 = RecordReader(tablePath2, **options)
  saveFile = files.open(savePath, 'w')

  records1 = recReader1.getRecords()
  records2 = recReader2.getRecords()
  while True:
    if len(records1) == 0 and len(records2) == 0:
      break
    elif len(records1) == 0:
      triangulate.writeRecords(saveFile, records2)
      records2 = recReader2.getRecords()
      continue
    elif len(records2) == 0:
      triangulate.writeRecords(saveFile, records1)
      records1 = recReader1.getRecords()
      continue

    key1 = records1[0].src + ' |||'
    key2 = records2[0].src + ' |||'
    if key1 < key2:
      triangulate.writeRecords(saveFile, records1)
      records1 = recReader1.getRecords()
    elif key1 > key2:
      triangulate.writeRecords(saveFile, records2)
      records2 = recReader2.getRecords()
    else: # key1 == key2
      merged = mergeRecords(records1, records2, **options)
      triangulate.writeRecords(saveFile, merged)
      records1 = recReader1.getRecords()
      records2 = recReader2.getRecords()
  saveFile.close()


def mergeRecords(*recListList, **options):
  RecordClass = options.get('RecordClass', MosesRecord)
  nbest = options.get('nbest', NBEST)

  merged = {}
  for records in recListList:
    for recNew in records:
      trgKey = recNew.trg + ' |||'
      if not trgKey in merged:
        recMerge = RecordClass()
        recMerge.src = recNew.src
        recMerge.trg = recNew.trg
        merged[trgKey] = recMerge
      else:
        recMerge = merged[trgKey]
      recMerge.counts.co += recNew.counts.co
      recMerge.aligns = set(recMerge.aligns) | set(recNew.aligns)
  if nbest > 0:
    if len(merged) > nbest:
      scores = []
      for key, rec in merged.items():
        scores.append( (rec.counts.co, key) )
      scores.sort(reverse = True)
      for count, key in scores[nbest:]:
        del merged[key]
  return merged


def integrate(table1, table2, lexfile, savefile, **options):
  # オプション初期値の設定
  RecordClass = options.get('RecordClass', MosesRecord)
  prefix = options.get('prefix', 'phrase')
  nbest     = options.get('nbest', NBEST)
  workdir   = options.get('workdir', '.')

  # 作業ディレクトリの作成
  workdir = workdir + '/integrate'
  files.mkdir(workdir)
  mergePath = "%s/%s_merged" % (workdir, prefix)
  revPath   = "%s/%s_reversed" % (workdir, prefix)
  trgCountPath = "%s/%s_trg" % (workdir, prefix)
  revTrgCountPath = "%s/%s_trgrev" % (workdir, prefix)
  countPath = "%s/%s_pprob" % (workdir, prefix)
  # 共起回数を足しあわせながらマージする
  progress.log("merging records into: %s\n" % mergePath)
  integrateTablePair(table1, table2, mergePath, **options)
  progress.log("merged table\n")
  # 単語単位の翻訳確率をロードする
  progress.log("loading word trans probabilities\n")
  lexCounts = lex.loadWordPairCounts(lexfile)
  # テーブルを逆転させる
  progress.log("reversing %s table into: %s\n" % (prefix, revPath) )
  reverseTable(mergePath, revPath, RecordClass)
  progress.log("reversed table\n")
  # 逆転したテーブルで逆方向のフレーズ翻訳確率を求める
  progress.log("calculating reversed phrase trans probs into: %s\n" % (trgCountPath))
  triangulate.calcPhraseTransProbsOnTable(revPath, trgCountPath, nbest = nbest, RecordClass = RecordClass)
  progress.log("calculated reversed phrase trans probs\n")
  # 再度テーブルを反転して元に戻す
  progress.log("reversing %s table into: %s\n" % (prefix,revTrgCountPath))
  reverseTable(trgCountPath, revTrgCountPath, RecordClass)
  progress.log("reversed table\n")
  # 順方向の翻訳確率を求める
  progress.log("calculating phrase trans probs into: %s\n" % (countPath))
  triangulate.calcPhraseTransProbsOnTable(revTrgCountPath, countPath, nbest = 0, RecordClass = RecordClass)
  progress.log("calculated phrase trans probs\n")
  # 語彙化翻訳確率を求める
  progress.log("calculating lex weights into: %s\n" % savefile)
  triangulate.calcLexWeights(countPath, lexCounts, savefile, RecordClass)
  progress.log("calculated lex weights\n")


def main():
  parser = argparse.ArgumentParser(description = 'load 2 phrase tables and pivot into one moses phrase table')
  parser.add_argument('table1', help = 'phrase table 1')
  parser.add_argument('table2', help = 'phrase table 2')
  parser.add_argument('lexfile', help = 'word pair counts file')
  parser.add_argument('savefile', help = 'path for saving moses phrase table file')
  parser.add_argument('--nbest', help = 'best n scores for phrase pair filtering (default = 20)', type=int, default=NBEST)
  parser.add_argument('--workdir', help = 'working directory', default='.')
  args = vars(parser.parse_args())

  integrate(**args)

if __name__ == '__main__':
  main()

