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
# cursor = hc.get_hive_cursor('172.18.140.8', 'default')
# sql = 'select test from test'
# res = hc.execute_sql(cursor, sql)[3][0]
#
#
# a = struct.unpack('i4hi4hi4hi4hi4hi4hi4hi4hi4hi4h', res[120000:120120])
# print(a)

# import asyncio
# import time
#
#
# now = lambda :time.time()
#
#
# async def do_some_work(x):
#     print("Waiting:",x)
#     # await asyncio.sleep(x)
#     await time.sleep(x)
#     return "Done after {}s".format(x)
#
# start = now()
#
# coroutine1 = do_some_work(1)
# coroutine2 = do_some_work(2)
# coroutine3 = do_some_work(4)
#
# tasks = [
#     asyncio.ensure_future(coroutine1),
#     asyncio.ensure_future(coroutine2),
#     asyncio.ensure_future(coroutine3)
# ]
#
# loop = asyncio.get_event_loop()
# loop.run_until_complete(asyncio.wait(tasks))
#
# for task in tasks:
#     print("Task ret:",task.result())
#
# print("Time:",now()-start)

from tools.analyse_stream import Read

file = './data/7ce82f8d-4dfb-4d74-a035-1fe2014bdc76_20180927170205.bin'
# file = './data/11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180808090648.0809.FSCAN'

for i in Read(file, 'fsc').header_payload():
    print(i)
    break