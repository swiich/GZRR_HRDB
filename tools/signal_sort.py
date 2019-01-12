import numpy as np
import hive_connector as hc
import datetime
import csv

file = open('test.csv', 'a')
writer = csv.writer(file)
index = 0

dtype = [('index', int), ('time_start', datetime.datetime), ('time_stop', datetime.datetime), ('freq', float),
         ('unknown1', float),('unknown2', float),('unknown3', float),('unknown4', int),('mfid', np.int64)]

cursor = hc.get_hive_cursor('172.39.8.60', 'analysesystem')
values = hc.execute_sql(cursor, 'select * from combined_signal order by mfid,freq,firsttime')

signal_array = np.array(values, dtype=dtype)
res = np.sort(signal_array, order=['mfid', 'freq', 'time_start'])
for i in range(len(res)-1):
    # 同mfid做比较
    first = res[i]
    second = res[i+1]
    if first['mfid'] == second['mfid']:
        # 判断频率差值是否在指定条件内
        if second['freq'] - first['freq'] > 50:

            index += 1
            first[0] = index
            print(first)
            writer.writerow(first)
        else:
            if (second['time_start']-first['time_start']).hour < 1:
                index += 1
                second[0] = index
                print(second)
                writer.writerow(second)
            else:
                index += 1
                first[0] = index
                first[3] = second[3]
                print(first)
                writer.writerow(first)
    else:
            
        index += 1
        first[0] = index
        print(first)
        writer.writerow(first)
index += 1
res[-1][0] = index
print(res[-1])
writer.writerow(res[-1])

file.close()
