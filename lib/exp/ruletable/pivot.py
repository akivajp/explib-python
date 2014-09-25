#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''Travatarのルールテーブルを2つ受け取り、1つのルールテーブルに合成する.

ピボット過程のデータはSQLite3 DBファイル形式で扱うが、
メモリファイルを指定することもできる'''

import argparse
from exp.ruletable import convert2sqlite, extract, triangulate

def pivot(**keys):
  # 事前にマルチプロセス処理のためのワークセットを準備
  workset = triangulate.WorkSet(keys['table_save'])
  # 1つ目のルールテーブルをソースDBとして保存する
  db = convert2sqlite.convert(keys['table_path1'], keys['dbfile'], keys['table_name1'])
  # 2つ目のルールテーブルをソースDBに追加保存する
  convert2sqlite.convert(keys['table_path2'], db, keys['table_name2'])
  # SQLite3 DBに保存された2つのルールテーブルを結合してピボット
  triangulate.IGNORE = keys['ignore']
  triangulate.pivot(workset, db, keys['table_name1'], keys['table_name2'])
  db.close()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'pivot 2 travatar rule table files into 1 rule table file')
  parser.add_argument('table_path1', help = 'file path of travatar rule table 1')
  parser.add_argument('table_path2', help = 'file path of travatar rule table 2')
  parser.add_argument('table_save', help = 'file path to save the pivoted rule table')
  parser.add_argument('--dbfile', help = 'file path to storing the 2 source rule tables (can be :memory:)', default=':memory:')
  parser.add_argument('--table_name1', help = 'name of the sql table for storing travatar rule table 1', default='rule1')
  parser.add_argument('--table_name2', help = 'name of the sql table for storing travatar rule table 2', default='rule2')
  parser.add_argument('--ignore', help = 'threshold for ignoring the rule translation probability (real number)',
                      type=float, default=triangulate.IGNORE)

  args = vars( parser.parse_args() )
  pivot(**args)

