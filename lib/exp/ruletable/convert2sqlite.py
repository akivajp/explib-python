#!/usr/bin/env python
# -*- encoding: utf-8 -*-

'''Travatar のルールテーブルファイルをロードし、SQlite3のDB形式に変換して保存する'''

import argparse
import sqlite3

# exp libraries
from exp.common import debug, files, progress
from exp.ruletable import sqlcmd

def convert(table_path, db_save, table_name):
  size = files.get_content_size(table_path)
  f_in = files.open(table_path, 'r')
  print("loading rule table file: %s" % table_path)
  progress.log("loading (0%): 0 records")
  if type(db_save) != sqlite3.Connection:
    db_save = sqlite3.connect(db_save)
  sqlcmd.drop_table(db_save, table_name)
  sqlcmd.create_table(db_save, table_name)
  sqlcmd.create_indices(db_save, table_name)
  c = progress.Counter(scaleup = 1000)
  for line in f_in:
    c.add(1)
    n = c.count
    if type(line) == bytes:
      line = line.decode('utf-8')
    fields = line.strip().split('|||')
    source = fields[0].strip()
    target = fields[1].strip()
    features = fields[2].strip()
    counts = fields[3].strip()
    align  = fields[4].strip()
    sqlcmd.insert_record(db_save, table_name, source, target, features, counts, align)
    if c.should_print():
      c.update()
      ratio = f_in.tell() * 100 / float(size)
      progress.log("loaded (%(ratio)3.2f%%): %(n)d records, last rule: '%(source)s'" % locals())
  progress.log("loaded (100%%): %(n)d records" % locals() )
  print('')
  db_save.commit()
  return db_save
  #db.close()


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'load travatar rule table format into sqlite3 dbfile')
  parser.add_argument('table_path', help = 'path to travatar rule table (rule-table.gz)')
  parser.add_argument('db_save', help = 'sqlite3 dbfile to store the rule table')
  parser.add_argument('table_name', help = 'table name to store the records in sqlite3 db')

  args = vars(parser.parse_args())
  convert(**args)

