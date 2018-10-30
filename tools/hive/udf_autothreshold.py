# coding=utf-8

import sys

i = 0

for line in sys.stdin:
    detail = line.strip().split('\t')
    print(detail[0])
    i += 1
print(i)
