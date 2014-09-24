#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''ファイル入出力関係の補助関数群'''

import gzip
import os.path
import re

from exp.common import debug

_open = open

def get_ext(filename):
  '''ファイルの拡張子を取得'''
  (root, ext) = os.path.splitext(filename)
  return ext

def open(filename, mode = 'r'):
  '''圧縮/非圧縮のファイルを透過的に開く'''
  if get_ext(filename) == '.gz':
    return gzip.open(filename, mode)
  else:
    return _open(filename, mode)

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

