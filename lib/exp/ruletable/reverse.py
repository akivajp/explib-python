#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''ルールテーブルを逆転させる'''

from exp.ruletable import record
from exp.phrasetable.reverse import reverseTable

def reverseTravatarTable(srcFile, saveFile):
  reverseTable(srcFile, saveFile, record.TravatarRecord)

