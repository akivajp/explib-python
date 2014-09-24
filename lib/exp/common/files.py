#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''ファイル入出力関係の補助関数群'''

import gzip
import io
import os.path
import re

from exp.common import debug
import exp.common.progress

_open = open

def get_content_size(path):
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

def get_ext(filename):
  '''ファイルの拡張子を取得'''
  (root, ext) = os.path.splitext(filename)
  return ext

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

def open(filename, mode = 'r'):
  '''圧縮/非圧縮のファイルを透過的に開く'''
  if get_ext(filename) == '.gz':
    return gzip.open(filename, mode)
  else:
    return _open(filename, mode)

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

