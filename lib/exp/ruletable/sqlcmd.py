#!/usr/bin/env python
# -*- encoding: utf-8 -*-

'''フレーズテーブル操作に関連するSQL文実行関数群'''

def create_table(db, table):
  '''フレーズテーブル格納用のSQLテーブル作成'''
  db.execute('''
    CREATE TABLE IF NOT EXISTS %(table)s (
      id integer primary key,
      source text,
      target text,
      features text,
      counts text,
      alignments text
    );
  ''' % locals() )

def create_indices(db, table):
  '''フレーズ検索用のインデックス作成 '''
  db.executescript('''
    CREATE INDEX %(table)s_source ON %(table)s(source);
    CREATE INDEX %(table)s_target ON %(table)s(target);
    CREATE UNIQUE INDEX %(table)s_both ON %(table)s(source, target);
  ''' % locals() )

def select_pivot(db, table1, table2):
  '''ピボットされたフレーズ対応を探す

  table1.target = table2.sourceとなるような全フレーズ対応を選択
  '''
  return db.execute('''
    SELECT %(table1)s.source, %(table1)s.target, %(table2)s.target, %(table1)s.features, %(table2)s.features,
           %(table1)s.counts, %(table2)s.counts, %(table1)s.alignments, %(table2)s.alignments
      FROM %(table1)s INNER JOIN %(table2)s ON %(table1)s.target = %(table2)s.source
  ''' % locals() )

def select_sorted(db, table):
  '''全レコードを source, target の順にソートして選択'''
  return db.execute('''
    SELECT source, target, features, counts, alignments
      FROM %(table)s
      ORDER BY source, target
  ''' % locals() )

def insert_record(db, table, source, target, features, counts, align):
  '''テーブルにレコードを挿入する'''
  if type(source) == bytes:
    source = source.decode('utf-8')
  if type(target) == bytes:
    target = target.decode('utf-8')
  sql = '''
    INSERT INTO %(table)s VALUES (
      null,
      ?,
      ?,
      "%(features)s",
      "%(counts)s",
      "%(align)s"
    );
  ''' % locals()
  db.execute(sql, (source, target) )

def drop_table(db, table_name):
  '''テーブルの削除'''
  db.execute('''
    DROP TABLE IF EXISTS %(table_name)s
  ''' % locals() )

