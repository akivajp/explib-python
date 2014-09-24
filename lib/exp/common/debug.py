#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''デバッグプリント用の補助関数群'''

import inspect
import sys

_debugging = True

def _show_caller():
  s = inspect.stack()[2]
  frame    = s[0]
  filename = s[1]
  line     = s[2]
  name     = s[3]
  code     = s[4]
  if code:
    sys.stdout.write("[%s:%s] %s: " % (filename, line, code[0].strip() ))
  else:
    sys.stdout.write("[%s:%s] : " % (filename, line) )

def log(*args, **keys):
  '''与えられた引数の内容を、呼び出し元情報と一緒に表示する'''
  if _debugging:
    if not 'sep' in keys:
      keys['sep'] = ' '
    if not 'end' in keys:
      keys['end'] = "\n"
    _show_caller()
    sys.stdout.write( keys['sep'].join( map(str, args) ) )
    sys.stdout.write( keys['end'] )
    sys.stdout.flush()

def enable():
  global _debugging
  _debugging = True

def disable():
  global _debugging
  _debugging = False

