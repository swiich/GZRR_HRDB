

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

from tools.analyse_stream import Read

# file = './data/7adb8062-4ace-4269-ab11-6f019a9fe0db_20180928193211.bin'
# file = './data/7ce82f8d-4dfb-4d74-a035-1fe2014bdc76_20180927170205.bin'
file = './data/hr/11000001111111-838a7074-ff73-49c3-a65d-86dd0ec967dd.bin'

for i in Read(file).header_payload():
    print(i)
    # break

# from tools.file_info import MBasicDataTable
# for i in MBasicDataTable(file).header_payload():
#     print(i)
#     break