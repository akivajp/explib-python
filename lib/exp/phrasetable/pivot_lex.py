#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
from exp.phrasetable import lex

def pivot_lex(lexfile1, lexfile2, savefile):
  cntSrcPvt = lex.loadWordPairCounts(lexfile1)
  cntPvtTrg = lex.loadWordPairCounts(lexfile2)
  cntSrcTrg = lex.pivotWordPairCounts(cntSrcPvt, cntPvtTrg)
  lex.saveWordPairCounts(savefile, cntSrcTrg)

def main():
  parser = argparse.ArgumentParser(description = 'triangulate lex files by co-occurrence counts estimation')
  parser.add_argument('lexfile1', help = 'word pair counts file src->pvt')
  parser.add_argument('lexfile2', help = 'word pair counts file pvt->trg')
  parser.add_argument('savefile', help = 'path for saving word pair counts')
  args = vars(parser.parse_args())

  pivot_lex(**args)

if __name__ == '__main__':
  main()

