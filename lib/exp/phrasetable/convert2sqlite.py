#!/usr/bin/env python
# -*- encoding: utf-8 -*-

'''Moses のフレーズテーブルファイルをロードし、SQlite3のDB形式に変換して保存する'''

import argparse
import os
import sqlite3

# exp libraries
from exp.common import debug, files, progress
from exp.phrasetable import sqlcmd

def convert(table_path, db_save, table_name):
  f_in = files.load(table_path)
  print("loading phrase table file: %s" % table_path)
  progress.log("loading (0%): 0 records")

  if type(db_save) != sqlite3.Connection:
    db_save = sqlite3.connect(db_save)
  db_save.execute('PRAGMA synchronous = OFF')
  #db_save.execute('PRAGMA synchronous = NORMAL')
  db_save.execute('PRAGMA journal_mode = OFF')
  #db_save.execute('PRAGMA journal_mode = WAL')
  db_save.execute('PRAGMA temp_store = MEMORY')
  #db_save.execute('PRAGMA cache_size = 100000')
  db_save.execute('PRAGMA cache_size = 500000')
  #db_save.execute('PRAGMA default_cache_size = 100000')
  db_save.execute('PRAGMA default_cache_size = 500000')
  db_save.execute('PRAGMA locking_mode = EXCLUSIVE')

  sqlcmd.drop_table(db_save, table_name)
  sqlcmd.create_table(db_save, table_name)
  sqlcmd.create_indices(db_save, table_name)
  c = progress.Counter(scaleup = 1000, limit = os.path.getsize(table_path) )
  n = 0
  for line in f_in:
    n += 1
    c.count = files.rawtell( f_in )
    if type(line) == bytes:
      line = line.decode('utf-8')
    fields = line.strip().split('|||')
    source = fields[0].strip()
    target = fields[1].strip()
    scores = fields[2].strip()
    align  = fields[3].strip()
    counts = fields[4].strip()
    sqlcmd.insert_record(db_save, table_name, source, target, scores, align, counts)
    if c.should_print():
      c.update()
      ratio = c.ratio * 100
      progress.log("loaded (%(ratio)3.2f%%): %(n)d records, last phrase: '%(source)s'" % locals())
  progress.log("loaded (100%%): %(n)d records" % locals() )
  print('')
  db_save.commit()
  return db_save
  #db.close()


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'load moses phrase table format into sqlite3 dbfile')
  parser.add_argument('table_path', help = 'path to moses phrase table (phrase-table.gz)')
  parser.add_argument('db_save', help = 'sqlite3 dbfile to store the phrase table')
  parser.add_argument('table_name', help = 'table name to store the records in sqlite3 db')

  args = vars(parser.parse_args())
  convert(**args)

