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
      scores text,
      alignment text,
      counts text
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
    SELECT %(table1)s.source, %(table1)s.target, %(table2)s.target, %(table1)s.scores, %(table2)s.scores,
           %(table1)s.alignment, %(table2)s.alignment, %(table1)s.counts, %(table2)s.counts
      FROM %(table1)s INNER JOIN %(table2)s ON %(table1)s.target = %(table2)s.source
  ''' % locals() )

def select_sorted(db, table):
  '''全レコードを source, target の順にソートして選択'''
  return db.execute('''
    SELECT source, target, scores, alignment, counts
      FROM %(table)s
      ORDER BY source, target
  ''' % locals() )

def insert_record(db, table, source, target, scores, align, counts):
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
      "%(scores)s",
      "%(align)s",
      "%(counts)s"
    );
  ''' % locals()
  db.execute(sql, (source, target) )

def drop_table(db, table_name):
  '''テーブルの削除'''
  db.execute('''
    DROP TABLE IF EXISTS %(table_name)s
  ''' % locals() )

