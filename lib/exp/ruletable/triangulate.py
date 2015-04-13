#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''2つのルールテーブルをピボット側で周辺化し、新しく1つのルールテーブルを合成する．'''

import argparse

# my exp libs
import exp.phrasetable.triangulate as base
from exp.ruletable.record import TravatarRecord

# デフォルト値の設定

# フレーズ対応の出力を打ち切る上限、自然対数値で指定する
#THRESHOLD = 1e-3
THRESHOLD = 0 # 打ち切りなし

# フィルタリングで残す数
#NBEST = 40
NBEST = 20

NULLS = 10**4

# 翻訳確率の推定方法 counts/probs
methods = base.methods
METHOD = 'countmin'

# 語彙翻訳確率の推定方法
#lexMethods = ['prodweight', 'countmin', 'prodprob', 'bidirmin', 'bidirmax', 'bidirgmean','table',
#              'countmin+table', 'prodprob+table', 'bidirmin+table', 'bidirgmean+table', 'bidirmax+table']
lexMethods = base.lexMethods
LEX_METHOD = 'prodweight'

def main():
  parser = argparse.ArgumentParser(description = 'load 2 rule tables and pivot into one travatar rule table')
  parser.add_argument('table1', help = 'rule table 1')
  parser.add_argument('table2', help = 'rule table 2')
  parser.add_argument('savefile', help = 'path for saving travatar rule table file')
  parser.add_argument('--threshold', help = 'threshold for ignoring the phrase translation probability (real number)', type=float, default=THRESHOLD)
  parser.add_argument('--nbest', help = 'best n scores for rule pair filtering (default = 20)', type=int, default=NBEST)
  parser.add_argument('--method', help = 'triangulation method', choices=methods, default=METHOD)
  parser.add_argument('--lexmethod', help = 'lexical triangulation method', choices=lexMethods, default=LEX_METHOD)
  parser.add_argument('--workdir', help = 'working directory', default='.')
  parser.add_argument('--alignlex', help = 'word pair counts file', default=None)
  parser.add_argument('--nulls', help = 'number of NULLs (lines) for table lex', type = int, default=NULLS)
  parser.add_argument('--noprefilter', help = 'No pre-filtering', type = bool, default=False)
  parser.add_argument('--multitarget', help = 'enabling multi target model', action='store_true')
  args = vars(parser.parse_args())

  args['RecordClass'] = TravatarRecord
  args['prefix'] = 'rule'
  base.pivot(**args)

if __name__ == '__main__':
  main()

