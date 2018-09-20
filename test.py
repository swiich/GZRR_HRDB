# from tools.analyse_stream import Read
#
# # file = './data/test.position'
# file = './data/test.Antennafactor'
#
# for i in Read(file, 'antenna').header_payload():
#     print(i)

# import struct
# from socket_d.hive import hive_connector as hc
#
# cursor = hc.get_hive_cursor('172.18.140.8', 'analysesystem')
# sql = 'select datacontent from business_spectrum_channel where staticstype == "3"'
# res = hc.execute_sql(cursor, sql)[0][0]
#
#
# a = struct.unpack('>i4hi4hi4hi4hi4hi4hi4hi4hi4hi4h', res[120000:120120])
# print(a)

# from tools.signal_handler import get_businessid
# start_freq = 30000.0
# stop_freq = 300000.0
# bid_tmp = get_businessid(start_freq, stop_freq)
# businessid = bid_tmp[0][0] if isinstance(bid_tmp, list) else bid_tmp
# print(businessid)

import struct

a = 1546541365465465
print(struct.pack('<q', a))
print(struct.pack('>q', a))
