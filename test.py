

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

# from tools.c_lib.c_invoker import CInvoker
# import hive_connector as hc
# from tools.analyse_stream import Read
# from tools.signal_handler import freq_avg

# file = '/home/data/s1/52010000_0002_20180809_171213_780MHz_980MHz_12.5kHz_V_F.bin'
# file = './data/000045CF-1449-55BD-05E4-31296C4E5C37_20181018160227.bin'
# framelist = []

# for i in Read(file).header_payload():
#     framelist.append(i)
    # print(i)
    # start_freq = i[1][4]
    # stop_freq = i[1][5]
    # step = i[1][7]
    # fp_data = i[1][-1]
    # break

# cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
# sql = "select * from spectrum_data limit 100"
# res = hc.execute_sql(cursor, sql)
# print(len(eval(res[0][-2])))
# so = CInvoker(fp_data, start_freq, stop_freq, step)
# auto = so.auto_threshold()
# print(fp_data)
# print(auto)



# print(framelist)

# for i in freq_avg(framelist, 10):
#     print(i)
#     break

# from tools.file_info import MBasicDataTable
# for i in MBasicDataTable(file).header_payload():
#     print(i)
#     break

# import hive_connector as hc
# import struct
#
# cursor = hc.get_hive_cursor('172.18.140.8', 'analysesystem')
# sql = "select datacontent from stats_specturm_area limit 1"
# res = hc.execute_sql(cursor, sql)[0][0]
# for i in range(0,len(res),12):
#     a = struct.unpack('i4h', res[i:i+12])
#     print(a)
#     # break

# import csv
#
# file = './data/station.csv'
# station = []
# with open(file,'r') as f:
#     reader = csv.reader(f)
#     for i in reader:
#         row = (float(i[-4]),float(i[-3]),float(i[-1]),i[-7])
#         station.append(row)
# station = tuple(station)
from spectrum_evaluate import SpectrumStatistics

file = './data/spectrumstatics/52260000_0001_20180904_104324_780MHz_980MHz_12.5kHz_V_M.bin'
count = 0
for i in SpectrumStatistics(file).resolve():
    print(i)
    break
    count += 1
print(count)