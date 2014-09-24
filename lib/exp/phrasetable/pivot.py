#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Mosesのフレーズテーブルを2つ受け取り、1つのフレーズテーブルに合成する.

ピボット過程のデータはSQLite3 DBファイル形式で扱うが、
メモリファイルを指定することもできる'''

import argparse
from exp.phrasetable import convert2sqlite, extract, triangulate

def pivot(**keys):
  # 1つ目のフレーズテーブルをソースDBとして保存する
  db_phrase = convert2sqlite.convert(keys['table_path1'], keys['db_phrase'], keys['table_name1'])
  # 2つ目のフレーズテーブルをソースDBに追加保存する
  convert2sqlite.convert(keys['table_path2'], db_phrase, keys['table_name2'])
  # SQLite3 DBに保存された2つのフレーズテーブルを結合してピボット
  triangulate.IGNORE = keys['ignore']
  db_pivot = triangulate.pivot(db_phrase, keys['table_name1'], keys['table_name2'], keys['db_pivot'],
                               keys['pivot_name'], keys['cores'])
  # SQlite3 DBからピボットされたフレーズテーブルを抽出
  extract.extract(db_pivot, keys['pivot_name'], keys['table_save'])

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'pivot 2 moses phrase table files into 1 phrase table file')
  parser.add_argument('table_path1', help = 'file path of moses phrase table 1')
  parser.add_argument('table_path2', help = 'file path of moses phrase table 2')
  parser.add_argument('table_save', help = 'file path to save the pivoted phrase table')
  parser.add_argument('--db_phrase', help = 'file path to storing the 2 source phrase tables (can be :memory:)', default=':memory:')
  parser.add_argument('--table_name1', help = 'name of the sql table for storing moses phrase table 1', default='phrase1')
  parser.add_argument('--table_name2', help = 'name of the sql table for storing moses phrase table 2', default='phrase2')
  parser.add_argument('--db_pivot', help = 'file path to storing the pivoted phrase tables (can be :memory:)', default=':memory:')
  parser.add_argument('--pivot_name', help = 'name of pivoted phrase table in sqlite db', default='pivot')
  parser.add_argument('--cores', help = 'number of processes parallel computing', type=int, default=1)
  parser.add_argument('--ignore', help = 'threshold for ignoring the phrase translation probability (real number)',
                      type=float, default=triangulate.IGNORE)

  args = vars( parser.parse_args() )
  pivot(**args)