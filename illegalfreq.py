# coding=utf-8

import hive_connector as hc
from math import pow
import sys

cursor = hc.get_hive_cursor('172.39.8.60', 'spectrum_evaluation')
sql = 'select * from ampdict'
res = hc.execute_sql(cursor, sql)
length = len(res)
count = 0
illegal = 0
for i in res:
    count += 1
    for k, v in eval(i[4]).items():
        if v/i[3] > 0.03:
            if (pow(10, (eval(i[5])[k]/10 - 107)/10)) / 1000 > i[-1]:
                with open('/illegal', 'a') as f:
                    f.write(str(k)+','+'1'+','+i[0]+','+i[2]+','+i[1])
                    f.write('\n')
                illegal += 1
    sys.stdout.write('\r当前扫描: '+str(count)+' / '+str(length)+'\t违规频点数量: '+str(illegal))
    sys.stdout.flush()
