#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''SQLite3データベースから、2つのフレーズテーブルをジョイントし、
確率スコアの周辺化を行うことで、新しく1つのフレーズテーブルを合成する．'''

import argparse
import codecs
import multiprocessing
import sqlite3
import sys

# my exp libs
from exp.common import debug, files, progress
from exp.phrasetable import sqlcmd

#IGNORE = 1e-3
IGNORE = 1e-2

class WorkSet:
  '''マルチプロセス処理に必要な情報をまとめたもの'''
  def __init__(self, savefile):
    f_out = files.open(savefile, 'wb')
    self.fileobj = codecs.getwriter('utf-8')(f_out)
    self.pivot_count = progress.Counter(scaleup = 1000)
    self.record_queue = multiprocessing.Queue()
    self.pivot_queue  = multiprocessing.Queue()
    self.marginalizer = multiprocessing.Process( target = marginalize, args=(self,) )
    self.recorder = multiprocessing.Process( target = write_records, args = (self,) )

  def __del__(self):
    self.close()

  def close(self):
    if self.marginalizer.pid:
      if self.marginalizer.exitcode == None:
        self.marginalizer.terminate()
      self.marginalizer.join()
    if self.recorder.pid:
      if self.recorder.exitcode == None:
        self.recorder.terminate()
      self.recorder.join()
    self.record_queue.close()
    self.pivot_queue.close()
    self.fileobj.close()

  def join(self):
    self.marginalizer.join()
    self.recorder.join()

  def start(self):
    self.marginalizer.start()
    self.recorder.start()

  def terminate(self):
    self.marginalizer.terminate()
    self.recorder.terminate()


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

def marginalize(workset):
  '''条件付き確率の周辺化を行うワーカー関数

  ピボット対象のレコードの配列を record_queue で受け取り、処理したデータを pivot_queue で渡す'''
  row_count = 0
  while True:
    # 処理すべきレコード配列を発見
    rows = workset.record_queue.get()
    if rows == None:
      # None を受け取ったらプロセス終了
      break
    #debug.log(len(rows))

    records = {}
    source = ''
    for row in rows:
      row_count += 1
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
    # 周辺化したレコードをキューに追加して、別プロセスに書き込んでもらう
    if records:
      workset.pivot_count.add( len(records) )
      for pair in sorted(records.keys()):
        rec = records[pair]
        workset.pivot_queue.put([ pair[0], pair[1], rec[0], rec[1], rec[2] ])
      if workset.pivot_count.should_print():
        workset.pivot_count.update()
        progress.log("processing %d records, pivoted %d records, last phrase: '%s'" %
                     (row_count, workset.pivot_count.count, source))
  # while ループを抜けた
  progress.log("processed %d records, pivoted %d records" % (row_count, workset.pivot_count.count))
  print('')
  # write_records も終わらせる
  workset.pivot_queue.put(None)

def write_records(workset):
  '''キューに溜まったピボット済みのレコードをファイルに書き出す'''
  while True:
    rec = workset.pivot_queue.get()
    if rec == None:
      # Mone を受け取ったらループ終了
      break
    source = rec[0]
    target = rec[1]
    scores = str.join(' ', map(str, rec[2]))
    align  = str.join(' ', sorted(rec[3].keys()) )
    counts = str.join(' ', map(str, rec[4]) )
    buf  = source + ' ||| '
    buf += target + ' ||| '
    buf += scores + ' ||| '
    buf += align  + ' |||'
    #buf += counts + ' |||'
    buf += "\n"
    workset.fileobj.write(buf)

def pivot(workset, db_src, table1, table2):
  # 周辺化を行う対象フレーズ
  # curr_rule -> pivot_rule -> target の形の訳出を探す
  try:
    if type(db_src) != sqlite3.Connection:
      files.test( db_src )
      db_src = sqlite3.connect(db_src)
    workset.start()
    cur = sqlcmd.select_pivot(db_src, table1, table2)
    curr_rule = ''
    rows = []
    for row in cur:
      #print(row)
      source = row[0]
      if curr_rule != source:
        # 新しい原言語フレーズが出てきたので、ワーカープロセスに処理してもらう
        workset.record_queue.put(rows)
        rows = []
        curr_rule = source
      rows.append(row)
    else:
      # 最後のデータ処理
      workset.record_queue.put(rows)
      workset.record_queue.put(None)
    # 書き出しプロセスの正常終了待ち
    workset.join()
    # ワークセットを片付ける
    workset.close()
  except KeyboardInterrupt:
    # 例外発生、全てのワーカープロセスを停止させる
    print('')
    print('Caught KeyboardInterrupt, terminating all the worker processes')
    workset.close()

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description = 'load 2 phrase tables from sqlite3 and pivot into sqlite3 table')
  parser.add_argument('db_src', help = 'sqlite3 dbfile including following source tables')
  parser.add_argument('table1', help = 'table name for task 1 of moses phrase-table')
  parser.add_argument('table2', help = 'table name for task 2 of moses phrase-table')
  parser.add_argument('savefile', help = 'path for saving travatar rule table file')
  parser.add_argument('--ignore', help = 'threshold for ignoring the phrase translation probability (real number)', type=float, default=IGNORE)
  args = vars(parser.parse_args())

  IGNORE = args['ignore']
  del args['ignore']
  workset = WorkSet(args['savefile'])
  del args['savefile']
  pivot(workset = workset, **args)

