#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''フレーズテーブルを反転させる'''

import codecs
import pprint
import subprocess
from collections import defaultdict

from exp.common import files
from exp.phrasetable import record

pp = pprint.PrettyPrinter()

def reverseTable(srcFile, saveFile, RecordClass = record.MosesRecord):
  if type(srcFile) == str:
    srcFile = files.open(srcFile)
  if type(saveFile) == str:
    if files.getExt(saveFile) == '.gz':
      saveFile = open(saveFile, 'w')
      pipeGzip = subprocess.Popen(['gzip'], stdin=subprocess.PIPE, stdout=saveFile)
      saveFile = pipeGzip.stdin
    else:
      saveFile = open(saveFile, 'w')
  pipeSort = subprocess.Popen(['sort'], env={"LC_ALL":"C"}, stdin=subprocess.PIPE, stdout=saveFile)
#  inputSort = codecs.getwriter('utf-8')(pipeSort.stdin)
  inputSort = pipeSort.stdin
  for line in srcFile:
    rec = RecordClass(line)
    inputSort.write( rec.getReversed().toStr() )
  pipeSort.stdin.close()
  pipeSort.communicate()
  saveFile.close()

def reverseMosesTable(srcFile, saveFile):
  reverseTable(srcFile, saveFile, record.MosesRecord)

