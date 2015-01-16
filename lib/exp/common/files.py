#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''ファイル入出力関係の補助関数群'''

import codecs
import gzip
import io
import os
import os.path
import re

from exp.common import debug
import exp.common.progress

_open = open

def autoCat(filenames, target):
  '''ファイルの内容をコピーする。圧縮ファイルであれば展開する'''
  if type(filenames) != list:
    filenames = [filenames]
  f_out = open(target, 'w')
  for filename in filenames:
    f_in = open(filename, 'r')
    for line in f_in:
      f_out.write(line)
    f_in.close()
  f_out.close()

def getContentSize(path):
  '''圧縮/非圧縮のファイルのサイズを透過的に調べる'''
  try:
    f_in = _open(path, 'rb')
    if get_ext(path) == '.gz':
      f_in.seek(-8, 2)
      crc32 = gzip.read32(f_in)
      isize = gzip.read32(f_in)
      f_in.close()
      return isize
    else:
      f_in.seek(0, 2)
      pos = f_in.tell()
      f_in.close()
      return pos
  except Exception as e:
    return -1

def getExt(filename):
  '''ファイルの拡張子を取得'''
  (name, ext) = os.path.splitext(filename)
  return ext

def isGzipped(filename):
  '''指定されたファイルがGzipで圧縮されているかどうか'''
  try:
    f = gzip.open(filename, 'r')
    f.readline()
    return True
  except Exception as e:
    return False

def load(filename, progress = True, bs = 10 * 1024 * 1024):
  '''ファイルをメモリ上に全て読み出し、圧縮されたファイルは透過的に読み込めるようにする'''
  data = io.BytesIO()
  f_in = _open(filename, 'rb')
  if progress:
    print("loading file: '%(filename)s'" % locals())
    c = exp.common.progress.Counter( limit = os.path.getsize(filename) )
  while True:
    buf = f_in.read(bs)
    if not buf:
      break
    else:
      data.write(buf)
      if progress:
        c.count = data.tell()
        if c.should_print():
          c.update()
          exp.common.progress.log('loaded (%3.2f%%) : %d bytes' % (c.ratio * 100, c.count))
  f_in.close()
  data.seek(0)
  if progress:
    exp.common.progress.log('loaded (100%%): %d bytes' % (c.count))
    print('')
  if get_ext(filename) == '.gz':
    f_in = gzip.GzipFile(fileobj = data)
    f_in.myfileobj = data
    return f_in
  else:
    return data

def mkdir(dirname, **options):
  '''ディレクトリを再帰的に作成する。ディレクトリが存在してもエラーを出さないが、ファイルなら例外'''
  if os.path.isdir(dirname):
    return
  else:
    ops = {}
    if options.has_key('mode'):
      ops['mode'] = options['mode']
    os.makedirs(dirname, **ops)

def open(filename, mode = 'r'):
  '''圧縮/非圧縮のファイルを透過的に開く'''
  if getExt(filename) == '.gz' or isGzipped(filename):
    fileObj = gzip.open(filename, mode)
  else:
    fileObj = _open(filename, mode)
  if mode.find('r') >= 0:
    return codecs.getreader('utf-8')(fileObj)
  else:
    return codecs.getwriter('utf-8')(fileObj)

def rawtell(fileobj):
  '''透過的にファイルの現在位置を求める．圧縮されたデータの場合は圧縮データバッファの位置を求める'''
  if type(fileobj) == gzip.GzipFile:
    return fileobj.myfileobj.tell()
  else:
    return fileobj.tell()

def test(filename):
  '''ファイルの存在チェック

  存在しない場合はそのままビルトイン open 関数に例外を投げてもらう'''
  f_in = _open(filename)
  f_in.close()
  return True

