#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys

def save_indices(table_file, index_file):
  fobj_table = open(table_file, 'r')
  fobj_index = open(index_file, 'w')
  while True:
    pos = fobj_table.tell()
    if fobj_table.readline() == '':
      break
    fobj_index.write("%(pos)s\n" % locals())

def make_indices(table_file):
  fobj = open(table_file, 'r')
  indices = []
  while True:
    pos = fobj.tell()
    if fobj_table.readline() == '':
      break
    indices.append( pos )
  return indices

def load_indices(index_file):
  fobj = open(index_file, 'r')
  indices = []
  for line in fobj:
    indices.append( int(line.strip()) )
  return indices

def get_record(fobj, indices, index):
  pos = indices[index]
  fobj.seek(pos)
  return fobj.readline().strip()

def get_src(record):
  fields = record.split('|||')
  return fields[0].strip()

# フレーズの共通するレコードをリストで返す
def get_common(fobj, indices, index):
  rec = get_record(fobj, indices, index)
  src = get_src(rec)
  records = [ rec ]
  i = 1
  while True:
    if index - i < 0:
      break
    rec = get_record(fobj, indices, index - i)
    if get_src(rec) == src:
      records.insert(0, rec)
      i += 1
    else:
      break
  i = 1
  while True:
    if index + i >= len(indices):
      break
    rec = get_record(fobj, indices, index + i)
    if get_src(rec) == src:
      records.append(rec)
      i += 1
    else:
      break
  return records

def indexed_binsearch(fobj_table, indices, src_phrase):
  #fobj_table = open(table_file, 'r')
  #src_phrase = unicode(src_phrase)
  def binsearch(start, end):
    #print(start, end, src_phrase, len(indices) )
    if start > end or start < 0 or end >= len(indices):
      return []
    if start == end:
      rec = get_record(fobj_table, indices, start)
      if get_src(rec) == src_phrase:
        return get_common(fobj_table, indices, start)
      else:
        return []
    mid = (start + end) / 2
    mid_rec = get_record(fobj_table, indices, mid)
    mid_src = get_src(mid_rec)
    #print(mid_src)
    if mid_src == src_phrase:
      return get_common(fobj_table, indices, mid)
    elif mid_src < src_phrase:
      return binsearch(mid + 1, end)
    else:
      return binsearch(start, mid - 1)
  return binsearch(0, len(indices) - 1)

if __name__ == '__main__':
  fobj = open(sys.argv[2])
  indices = make_index.load_indices
  found = indexed_binsearch(sys.argv[1], indices, sys.argv[3])
  print(found)

