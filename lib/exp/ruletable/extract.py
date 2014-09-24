#!/usr/bin/env python3
# encoding: utf-8

'''SQLite3 DBで保存されたピボット済みのルールテーブルをTravatar形式で書き出す'''

import argparse
import codecs
import gzip
import sqlite3
import sys

# my exp libs
from exp.common import debug, files, progress
from exp.ruletable import sqlcmd

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
    rec  = row[0] + ' ||| ' # source
    rec += row[1] + ' ||| ' # target
    rec += row[2] + ' ||| ' # features
    rec += row[3] + ' ||| ' # counts
    rec += row[4] # alignment
    rec += "\n"
    f_out.write( rec )
    if c.should_print():
      c.update()
      progress.log("saved %d records, last rule: '%s'" % (c.count, source))
  f_out.close()
  progress.log("saved %d records" % (c.count) )
  print('')

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'load rule tables from sqlite3 and extract into travatar rule table format')
  parser.add_argument('db_src', help = 'sqlite3 dbfile including pivoted rule table')
  parser.add_argument('table', help = 'table name of pivoted rule table')
  parser.add_argument('savefile', help = 'path for saving travatar rule table file')

  args = vars(parser.parse_args())
  extract(**args)

