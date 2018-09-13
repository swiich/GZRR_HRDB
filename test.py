# from tools.analyse_stream import Read
#
# file = './data/test.position'
# # file = './data/test.Antennafactor'
#
# for i in Read(file, 'gps').header_payload():
#     print(i)




# import random
# from socket_d.hive import hive_connector as hc
#
# cursor = hc.get_hive_cursor('172.39.8.62', 'db_data_store')
# sql = 'select * from amp_info where statstime=="2018-8-1 15:29:0"'
# res = hc.execute_sql(cursor, sql)
#
# content = []
# for j in res:
#     test_dict = eval(j[5])
#     sorted_key_list = sorted(test_dict)
#     sorted_dict = list(map(lambda x: {x: test_dict[x]}, sorted_key_list))
#     amp_middle = sorted_dict[int(len(sorted_dict)/2)].keys()
#     for i in amp_middle:
#         amp_middle = i
#     amp_max = sorted_dict[-1].keys()
#     for i in amp_max:
#         amp_max = i
#     occupancy = round(j[6]/res[0][7], 2) * 100
#     index = j[4]
#     freq_value = 30000000+index*25000
#     amp_threshold = round(random.uniform(-20, 40), 1)
#     content.append((index, freq_value, amp_max, amp_middle, occupancy, amp_threshold))
#     print(index)
#
# bno = '7f6888c6-b8ff-4782-9613-ce1fe73dc211'
# # areacode = '520100'
# stime = '2018-9-10 16:35'
# stype = '1'
# # station_count = 100
# # active_station = 71
# # devicecoverage = 71.00
# # freq_band_ocy = 10.25
# #
# # with open('stats_spectrum_area', 'w') as f:
# #     w = csv.writer(f)
# #     w.writerow((bno,areacode,stime,stype,station_count,active_station,devicecoverage,freq_band_ocy,content))
#
# def encode(s):
#     return ' '.join([bin(ord(c)).replace('0b', '') for c in s])
#
#
# mfid = 52010001119001
# amp_unit = '0'
# antennafactor = [i for i in range(10800)]
#
# with open('business_spectrum_channel', 'w') as f:
#     f.write('{0}|{1}|{2}|{3}|{4}|{5}|{6}'.format(mfid, stime, stype, bno, amp_unit, antennafactor, encode(str(content))))
# # a = str(content)
# # with open('test.b', 'w') as f:
# #     f.write(encode(a))

import struct

a = 0
with open('amp.bin', 'wb') as f:
    f.write()
    # for i in range(100):
    #     f.write(struct.pack('i', a))
    #     f.write(struct.pack('4h', a,a,a,a))