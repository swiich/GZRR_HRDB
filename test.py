

# import struct
# import time
# from socket_d.hive import hive_connector as hc
# start = time.time()
# cursor = hc.get_hive_cursor('172.18.140.8', 'default')
# sql = "SELECT   SUM(datastorage) AS datastorage, SUM(monitortime) AS summonitortime, SUM(consisttime) AS consisttime,  SUM(assignstationnum) AS assignstationnum, SUM(activestationnum)AS activestationnum, MAX(businessmaxocy) AS BUSINESSMAXOCY, MAX(monitortime) AS MONITORTIME FROM  analysesystem.statistics_business_spectrum where substr(MFID,0,6)='520100' and STATSTYPE='3' and STATSTIME>= '2018' and STATSTIME<='2018' and (businessno = '2a98e49e-bc86-11e8-a643-7cd30a552050')"
# res = hc.execute_sql(cursor, sql)
#
# end = time.time()
# print(end-start)
# print(res)

# from tools.analyse_stream import Read
# from tools.signal_handler import freq_avg

# file = './data/7adb8062-4ace-4269-ab11-6f019a9fe0db_20180928193211.bin'
# file = './data/0000434B-52B5-4805-43A2-1DF913B72394_20181019152125.bin'
# file = './data/000045CF-1449-55BD-05E4-31296C4E5C37_20181018160227.bin'
# file = './data/7adb8062-4ace-4269-ab11-6f019a9fe0db_20180928153703.bin'

# framelist = []
# for i in Read(file).header_payload():
    # framelist.append(i)
    # print(i)
# print(framelist)

# for i in freq_avg(framelist, 10):
#     print(i)
#     break

# from tools.file_info import MBasicDataTable
# for i in MBasicDataTable(file).header_payload():
#     print(i)
#     break

import hive_connector as hc
import struct

cursor = hc.get_hive_cursor('172.18.140.8', 'analysesystem')
sql = 'select datacontent from stats_specturm_area where freqregionocy=51.01 and startfreq=80000'
res, = hc.execute_sql(cursor, sql)[0]
for i in range(0,len(res),12):
    a = struct.unpack('i4h', res[i:i+12])
    print(a)

