#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''SQLite3データベースから、2つのルールテーブルをジョイントし、
確率スコアの周辺化を行うことで、新しく1つのルールテーブルを合成する．'''

import argparse
import codecs
import math
import multiprocessing
import sqlite3
import sys

# my exp libs
from exp.common import debug, files, progress
from exp.ruletable import sqlcmd

# フレーズ対応の出力を打ち切る上限、自然対数値で指定する
#IGNORE = -5.0
IGNORE = -7.0 # ほぼ 0.1%


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


def keyval_array(dic):
  '''辞書を、'key=val' という文字列要素で表した配列に変換する'''
  array = []
  for key, val in dic.items():
    array.append('%(key)s=%(val)s' % locals())
  return array

def log_add(features, features1, features2, key):
  '''ログ翻訳確率を、確率に戻して足しあわせて再びログを取る'''
  if not key in features:
    p = 0
  else:
    p = math.e ** features[key]
  log_p1 = float(features1[key])
  log_p2 = float(features2[key])
  p += (math.e ** (log_p1 + log_p2))
  features[key] = math.log(p)

def update_features(record, features1, features2):
  '''素性の更新'''
  features = record[0]
  # ログ翻訳確率は確率に戻して足し合わせて再度ログを取る
  log_add(features, features1, features2, 'fgep')
  log_add(features, features1, features2, 'egfp')
  log_add(features, features1, features2, 'fgel')
  log_add(features, features1, features2, 'egfl')
  # p と w は後の方を優先する
  features['p'] = features2['p']
  features['w'] = features2['w']

def update_counts(record, counts1, counts2):
  '''ルールの出現頻度を更新'''
  counts = record[1]
  for i, count1 in enumerate(counts1):
    count2 = counts2[i]
    counts[i] += (count1 * count2)

def merge_alignment(record, align1, align2):
  '''アラインメントのマージを試みる'''
  align = record[2]
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
    # 処理すべきレコード配列を取得
    rows = workset.record_queue.get()
    if rows == None:
      # None を受け取ったらプロセス終了
      break
    #debug.log(len(rows))

    records = {}
    source = ''
    for row in rows:
      row_count += 1
      #debug.log(row)
      source = row[0]
      pivot_rule = row[1] # 参考までに取得しているが使わない
      target = row[2]
      features1 = {}
      for keyval in row[3].split(' '):
        (key, val) = keyval.split('=')
        features1[key] = val
      features2 = {}
      for keyval in row[4].split(' '):
        (key, val) = keyval.split('=')
        features2[key] = val
      counts1 = [float(count) for count in row[5].split(' ')]
      counts2 = [float(count) for count in row[6].split(' ')]
      align1 = row[7].split()
      align2 = row[8].split()
      if not (source, target) in records:
        # 対象言語の訳出のレコードがまだ無いので作る
        records[(source, target)] = [ {}, [ 0.0, 0.0, 0.0 ], {} ]
      record = records[(source, target)]
      # 訳出のスコアを推定する
      update_features(record, features1, features2)
      # 句対応のカウントを更新
      update_counts(record, counts1, counts2)
      # アラインメントのマージ
      merge_alignment(record, align1, align2)
    # 非常に小さな翻訳確率のフレーズは無視する
    ignoring = []
    for (source, target), rec in records.items():
      if rec[0]['fgep'] < IGNORE and rec[0]['egfp'] < IGNORE:
        #print("\nignoring '%(source)s' -> '%(target)s' %(rec)s" % locals())
        ignoring.append( (source, target) )
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
        progress.log("processing %d records, pivoted %d records, last rule: %s" %
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
    features = str.join(' ', keyval_array(rec[2]))
    counts = str.join(' ', map(str, rec[3]) )
    align  = str.join(' ', sorted(rec[4].keys()) )
    buf  = source + ' ||| '
    buf += target + ' ||| '
    buf += features + ' ||| '
    buf += counts + ' ||| '
    buf += align
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
        # 新しい原言語フレーズが出てきたので、ここまでのデータを開いてるプロセスに処理してもらう
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
  parser = argparse.ArgumentParser(description = 'load 2 rule tables from sqlite3 and pivot into moses phrase table')
  parser.add_argument('db_src', help = 'sqlite3 dbfile including following source tables')
  parser.add_argument('table1', help = 'table name for task 1 of travatar rule-table')
  parser.add_argument('table2', help = 'table name for task 2 of travatar rule-table')
  parser.add_argument('savefile', help = 'path for saving travatar rule table file')
  parser.add_argument('--ignore', help = 'threshold for ignoring the rule translation probability (real number)', type=float, default=IGNORE)
  args = vars(parser.parse_args())

  IGNORE = args['ignore']
  del args['ignore']
  workset = WorkSet(args['savefile'])
  del args['savefile']
#  pivot(**args)
  pivot(workset = workset, **args)

