
import numpy as np
from tools.analyse_stream import Read


class MBasicDataTable(Read):
    def header_payload(self):
        file = open(self.file_name, 'rb')
        while True:
            leader = file.read(4)
            if not leader:                # 判断是否读取到文件末
                break
            file.read(6)
            ts = [np.frombuffer(file.read(2), np.uint16)[0]]
            for i in np.frombuffer(file.read(5), np.uint8):
                ts.append(i)
            ts.append(np.frombuffer(file.read(2), np.uint16)[0])    # 年 月 日 时 分 秒 毫秒
            pl = np.frombuffer(file.read(4), np.uint32)[0]
            el = np.frombuffer(file.read(1), np.uint8)[0]            # 扩展帧长度

            ts_str = "{0}-{1}-{2} {3}:{4}:{5}.{6}".format(*ts)

            if el:
                file.read(el)                        # 扩展帧头ExHeader

            payload = self.get_last_time(file, pl)

            head = (ts_str, payload)
            yield head

        file.close()

    @staticmethod
    def get_last_time(file, payload_len):
        """频谱数据"""
        payload = 0
        while payload_len:

            dt = np.frombuffer(file.read(1), np.uint8)[0]
            dl = np.frombuffer(file.read(4), np.uint32)[0]

            file.read(dl)
            payload = dt

            payload_len -= dl + 5
        return payload


count = 0
for i in Read('52010000140032_D26A16B0-C155-11E8-8002-28D24452E1DB_00002287-299C-7996-2A76-2CE32C8A361F_20190326093820.bin').header_payload():
    print(i[1])
    count += 1
    if count == 40:
        break


# from tools.analyse_stream import Read
# import numpy as np
#
# file = '52010000140018_0c60a03e-b334-4bec-9dcd-9a7760175739_4f4a1aa3-0ac4-44c3-a22a-08ad0854b7f1_20190228145113.bin'
#
#
# def freq_avg(frame_list, avg_count):
#     """
#
#     传入avg_count帧数据，返回平均值
#
#     """
#
#     np_data_total = []
#     counter = avg_count
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
#
#     for frame in frame_list_combined:
#         print(frame)
#         if not frame[1]:
#             continue
#
#         fp_data = np.array(list(map(lambda x: float(x) / 10, frame[1][-1])))
#         np_data_total.append(fp_data)
#
#         counter -= 1
#         if not counter:
#
#             tmp = np.zeros(shape=(1, len(np_data_total[0])))
#             for i in np_data_total:
#                 tmp += i
#
#             fp_data = (tmp/avg_count).round(1)
#
#             counter = avg_count
#             np_data_total = []
#
#             frame[1][-1] = fp_data[0]
#             yield frame
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
# frame_list = []
# counter = 0
# for i in Read(file).header_payload():
#     frame_list.append(i)
#     # print(i)
#     counter += 1
#     # if counter == 100:
#     #     break
#
# for frame in freq_avg(frame_list, 10):
#     print(frame)


