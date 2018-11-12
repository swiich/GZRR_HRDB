# coding=utf-8

import hive_connector as hc
import sys

cursor = hc.get_hive_cursor('172.39.8.60', 'spectrum_evaluation')
sql = 'select * from wrong_freq'
res = hc.execute_sql(cursor, sql)
length = len(res)
count = 0

file = open('illegal_freqband', 'a')
for data in res:
    data_list = eval(data[3])
    if data_list:
        e_type = data[2]
        if e_type == 0:
            staid = ''
        else:
            staid = data[-1]
        for freq in data_list:
            file.write(str(freq)+','+str(e_type)+','+data[0]+','+data[1]+','+staid)
            file.write('\n')
        count += 1
    sys.stdout.write('\r当前扫描: '+str(count)+' / '+str(length))
    sys.stdout.flush()
file.close()
sys.stdout.write('\n')