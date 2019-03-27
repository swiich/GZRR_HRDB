# def freq_avg(frame_list):
#     """
#
#     传入avg_count帧数据，返回平均值
#
#     """
#
#     # 合并被拆分的帧结构
#     frame_list_combined = []
#
#     # 整理数组使其第一帧为监测开始频率
#     while frame_list:
#         if frame_list[0][1][6] != 0:
#             frame_list.pop(0)
#         else:
#             break
#
#     # 如果第一帧为监测开始帧
#     if frame_list[0][1][8] == frame_list[0][1][3]:
#         frame_list_combined = frame_list
#     else:
#         while frame_list:
#
#             if not frame_list:
#                 break
#
#             current_frame = frame_list.pop(0)
#             # 监测频点为空
#             if not current_frame[1]:
#                 continue
#
#             current_channel_count = current_frame[1][8]
#
#             while current_channel_count < current_frame[1][3]:
#                 if not frame_list:
#                     break
#                 next_frame = frame_list.pop(0)
#                 current_channel_count += next_frame[1][8]
#                 tmp = combine_frame(current_frame, next_frame)
#                 current_frame = tmp
#             if current_frame and current_frame[1][3] == current_frame[1][8]:
#                 frame_list_combined.append(current_frame)
#     return frame_list_combined
#
#
# def combine_frame(*args):
#     """ 例如 87-108 被拆分为 87-106,106-108 两段或任意段，合并 """
#     levels = []
#     start_freq = args[0][1][4]
#     pl = 0
#     dl = 0
#     channel_total = 0
#     for i in range(len(args)):
#         pl += args[i][0][4]
#         dl += args[i][1][1]
#         channel_total += args[i][1][8]
#         stop_freq = args[i][1][5]
#         levels.extend(args[i][1][-1])
#     head = (args[0][0][0], args[0][0][1], args[0][0][2], args[-1][0][3], pl, args[0][0][5])
#     payload = [args[0][1][0], dl, args[0][1][2], args[0][1][3], start_freq, stop_freq, args[0][1][6], args[0][1][7], channel_total, levels]
#
#     result = (head, payload)
#     return result
#
# from tools.analyse_stream import Read
#
# file = '52010000140032_D26A16B0-C155-11E8-8002-28D24452E1DB_00002287-299C-7996-2A76-2CE32C8A361F_20190326093820.bin'
#
# frame_list = []
# for i in Read(file).header_payload():
#     frame_list.append(i)
#
# import struct
#
# b = freq_avg(frame_list)
# with open('test.bin', 'wb') as f:
#     for i in b:
#         for j in i[1][-1]:
#             f.write(struct.pack('h', j))
#
#
import struct

file = open('test.bin', 'rb')
while file:
    print(struct.unpack('h',file.read(2))[0])

