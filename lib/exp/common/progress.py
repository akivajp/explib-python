#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''処理途中の内容をコンソールに動的に書き換える'''

import sys

def _clean(n = 1):
  if n > 0:
    sys.stdout.write(' ' * n + "\b" * n)

def _len(buf):
  if sys.version_info.major >= 3:
    return len( bytes(buf, 'utf-8') )
  else:
    return len(buf)

_last_pos = 0
def log(*args, **keys):
  '''処理途中の内容をコンソールに動的に書き換える'''
  global _last_pos
  if not 'sep' in keys:
    keys['sep'] = ' '
  sys.stdout.write("\r")
  buf = keys['sep'].join( map(str, args) )
  sys.stdout.write( buf )
  count = _len(buf)
  _clean( (_last_pos - count) * 2 )
  _last_pos = count
  sys.stdout.flush()

