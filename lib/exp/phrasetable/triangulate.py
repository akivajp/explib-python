#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''SQLite3データベースから、2つのフレーズテーブルをジョイントし、
確率スコアの周辺化を行うことで、新しく1つのフレーズテーブルを合成する．'''

import argparse
import multiprocessing
import sqlite3
import sys

# my exp libs
from exp.common import debug, files, progress
from exp.phrasetable import sqlcmd

#IGNORE = 1e-3
IGNORE = 1e-2

procs = []

def insert_records(db, table_name, records):
  '''ピボットされたレコード配列をまとめてSQLite3テーブルに挿入する'''
  for (source, target), record in records.items():
    scores = str.join(' ', map(str, record[0]) )
    align  = str.join(' ', sorted(record[1].keys()) )
    # 出現頻度の推定も行いたいが非常に困難
    counts = None
    sql = '''
      INSERT INTO %(table_name)s VALUES (
        null,
        ?,
        ?,
        "%(scores)s",
        "%(align)s",
        null
      );
    ''' % locals()
    db.execute(sql, (source, target) )

def add_scores(record, scores1, scores2):
  '''スコアを掛けあわせて累積値に加算する'''
  scores = record[0]
  for i in range(0, len(scores)):
    scores[i] += scores1[i] * scores2[i]

def merge_alignment(record, align1, align2):
  '''アラインメントのマージを試みる'''
  align = record[1]
  a1 = {}
  for pair in align1:
    (left, right) = pair.split('-')
    if not left in a1:
      a1[left] = []
    a1[left].append(right)
  a2 = {}
  for pair in align2:
    (left, right) = pair.split('-')
    if not left in a2:
      a2[left] = []
    a2[left].append(right)
  for left in a1.keys():
    for middle in a1[left]:
      if middle in a2:
        for right in a2[middle]:
          pair = '%(left)s-%(right)s' % locals()
          align[pair] = True
#  if align != {'0-0': True}:
#    debug.log(align1, align2)
#    debug.log(a1, a2)
#    debug.log(align)

def empty_all(queues):
  for q in queues:
    #debug.log(q, q.empty())
    if not q.empty():
      return False
  return True

def get_empty_queue(queues):
  for i, p in enumerate(procs):
    if not p.is_alive():
      raise Exception('Process %(i)d was terminated' % locals() )
    q = queues[i]
    if q.empty():
      return q
  else:
    return None

def marginalize(record_queue, pivot_queue):
  '''条件付き確率の周辺化を行うワーカー関数

  ピボット対象のレコードの配列を record_queue で受け取り、処理したデータを pivot_queue で渡す'''
  while True:
    if not record_queue.empty():
      # 処理すべきレコード配列を発見
      rows = record_queue.get()
      #debug.log(len(rows))

      records = {}
      source = ''
      for row in rows:
        #print(row)
        source = row[0]
        pivot_phrase = row[1] # 参考までに取得しているが使わない
        target = row[2]
        scores1 = [float(score) for score in row[3].split(' ')]
        scores2 = [float(score) for score in row[4].split(' ')]
        align1 = row[5].split(' ')
        align2 = row[6].split(' ')
        counts1 = [int(count) for count in row[7].split(' ')]
        counts2 = [int(count) for count in row[8].split(' ')]
        if not (source, target) in records:
          # 対象言語の訳出のレコードがまだ無いので作る
          records[(source, target)] = [ [0, 0, 0, 0], {}, [0, 0, 0] ]
        record = records[(source, target)]
        # 訳出のスコア(条件付き確率)を掛けあわせて加算する
        add_scores(record, scores1, scores2)
        # アラインメントのマージ
        merge_alignment(record, align1, align2)
      # 非常に小さな翻訳確率のフレーズは無視する
      ignoring = []
      for (source, target), rec in records.items():
        if rec[0][0] < IGNORE and rec[0][2] < IGNORE:
          #print("\nignoring '%(source)s' -> '%(target)s' %(rec)s" % locals())
          ignoring.append( (source, target) )
        #elif rec[0][0] < IGNORE ** 2 or rec[0][2] < IGNORE ** 2:
        #  ignoring.append( (source, target) )
      for pair in ignoring:
        del records[pair]
      # 周辺化したレコードの配列を親プロセスに返す
      if records:
        #debug.log("finished pivoting, source phrase: '%(source)s'" % locals())
        #debug.log(source, len(rows), len(records))
        pivot_queue.put(records)


def flush_pivot_records(db_save, pivot_name, count, pivot_queue):
  '''キューに溜まったピボット済みのレコードをSQLで書き出す'''
  #print("flushing pivot records: %d" % pivot_queue.qsize())
  pivot_records = pivot_queue.get()
  for pair in pivot_records.keys():
    last = pair[0]
    break
  insert_records(db_save, pivot_name, pivot_records)
  count.add( len(pivot_records) )

def pivot(db_src, table1, table2, db_save, pivot_name, cores=1):
  global procs
  if type(db_src) != sqlite3.Connection:
    files.test( db_src )
    db_src = sqlite3.connect(db_src)
  if type(db_save) != sqlite3.Connection:
    db_save = sqlite3.connect(db_save)
  sqlcmd.drop_table(db_save, pivot_name)
  sqlcmd.create_table(db_save, pivot_name)
  sqlcmd.create_indices(db_save, pivot_name)

  record_queues = [multiprocessing.Queue() for i in range(0, cores)]
  pivot_queue = multiprocessing.Queue()
  procs = [multiprocessing.Process(target=marginalize, args=(record_queues[i], pivot_queue)) for i in range(0, cores)]
  for p in procs:
    p.start()

  # 周辺化を行う対象フレーズ
  # curr_phrase -> pivot_phrase -> target の形の訳出を探す
  try:
    cur = sqlcmd.select_pivot(db_src, table1, table2)
    curr_phrase = ''
    c = progress.Counter(scaleup = 1000)
    rows = []
    row_count = 0
    for row in cur:
      #print(row)
      source = row[0]
      if curr_phrase != source:
        # 新しい原言語フレーズが出てきたので、ここまでのデータを開いてるプロセスに処理してもらう
        while True:
          q = get_empty_queue(record_queues)
          if q:
            #debug.log(q)
            break
          if not pivot_queue.empty():
            flush_pivot_records(db_save, pivot_name, c, pivot_queue)
        q.put(rows)
        rows = []
        curr_phrase = source
      row_count += 1
      rows.append(row)
      if not pivot_queue.empty():
        # ワーカーがピボットしたレコードがあるので書き出す
        flush_pivot_records(db_save, pivot_name, c, pivot_queue)
        if c.should_print():
          c.update()
          progress.log("processing %d records, pivoted %d records, last phrase: '%s'" %
                       (row_count, c.count, source))
    else:
      # 最後のデータ処理
      while True:
        q = get_empty_queue(record_queues)
        if q:
          break
      q.put(rows)
    # すべてのワーカープロセスの終了（全てのキューが空になる）まで待つ
    while not empty_all(record_queues):
      pass
    # ワーカープロセスを停止させる
    for p in procs:
      p.terminate()
      p.join()
    # ピボットキューの残りを全て書き出す
    while not pivot_queue.empty():
      flush_pivot_records(db_save, pivot_name, c, pivot_queue)
    progress.log("processed %d records, pivoted %d records" % (row_count, c.count))
    print('')
  except Exception as e:
    # 例外発生、全てのワーカープロセスを停止させる
    print('')
    debug.log(e)
    print('terminating all the worker processes')
    for p in procs:
      if p.is_alive():
        p.terminate()
        p.join()
    p = []
    raise e
  db_save.commit()
  return db_save

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'load 2 phrase tables from sqlite3 and pivot into sqlite3 table')
  parser.add_argument('db_src', help = 'sqlite3 dbfile including following source tables')
  parser.add_argument('table1', help = 'table name for task 1 of moses phrase-table')
  parser.add_argument('table2', help = 'table name for task 2 of moses phrase-table')
  parser.add_argument('db_save', help = 'sqlite3 dbfile to result storing (can be the same with src_dbfile)')
  parser.add_argument('pivot_name', help = 'table name for pivoted phrase-table')
  parser.add_argument('--cores', help = 'number of processes parallel computing', type=int, default=1)
  parser.add_argument('--ignore', help = 'threshold for ignoring the phrase translation probability (real number)', type=float, default=IGNORE)
  args = vars(parser.parse_args())
  #debug.log(args)

  IGNORE = args['ignore']
  del args['ignore']
  pivot(**args)

