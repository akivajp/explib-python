#!/usr/bin/env python3
# encoding: utf-8

'''SQLite3 DBで保存されたピボット済みのフレーズテーブルをMoses形式で書き出す'''

import argparse
import codecs
import gzip
import sqlite3
import sys

# my exp libs
from exp.common import debug, files, progress
from exp.phrasetable import sqlcmd

def extract(db_src, table, savefile):
  if type(db_src) != sqlite3.Connection:
    files.test( db_src )
    db_src = sqlite3.connect(db_src)
  rows = sqlcmd.select_sorted(db_src, table)

  f_out = files.open(savefile, 'wb')
  f_out = codecs.getwriter('utf-8')(f_out)
  c = progress.Counter()
  for row in rows:
    c.add(1)
    source = row[0]
    rec  = row[0] + ' ||| '
    rec += row[1] + ' ||| '
    rec += row[2] + ' ||| '
    rec += row[3] + ' |||'
    # 出現頻度の推定も行いたいが非常に困難
    #rec += str.join(' ', map(str, record[2])) + ' |||'
    rec += "\n"
    f_out.write( rec )
    if c.should_print():
      c.update()
      progress.log("saved %d records, last phrase: '%s'" % (c.count, source))
  f_out.close()
  progress.log("saved %d records" % (c.count) )
  print('')

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'load phrase tables from sqlite3 and extract into moses phrase table format')
  parser.add_argument('db_src', help = 'sqlite3 dbfile including pivoted phrase table')
  parser.add_argument('table', help = 'table name of pivoted phrase table')
  parser.add_argument('savefile', help = 'path for saving moses phrase table file')

  args = vars(parser.parse_args())
  extract(**args)

