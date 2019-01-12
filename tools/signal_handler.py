import numpy as np
from tools.analyse_stream import Read
import csv
import uuid
import hive_connector as hc
import time
from tools.c_lib.c_invoker import CInvoker
import shutil


class AmpStruct:
    def __init__(self, index=0):
        """

        sig_index: 频点索引号
        amp_dict: 幅度值字典
        occupancy: 频点超过门限次数
        threshold_avg: 门限均值

        """
        self.sig_index = index
        self.amp_dict = {}
        self.occupancy = 0
        self.threshold_avg = 0


def single_signal_return(sig_count, sigdetectres):
    """

    返回单条信号信息

    """
    for i in range(sig_count):
        cf = sigdetectres[0][i]
        cfi = int(sigdetectres[1][i])
        cfa = sigdetectres[2][i]
        snr = sigdetectres[3][i]
        sb = sigdetectres[4][i]

        yield cf, cfi, cfa, snr, sb


def signal_to_csv(mfid, time_, sig_count, sigdetectres):
    """

    将调用clib后信息写入文件

    """
    res = single_signal_return(sig_count, sigdetectres)
    with open('/var/dropzone/signal.tmp', 'a') as f:
        w = csv.writer(f)
        for i in res:
            tmp = (mfid, time_, *i)
            w.writerow(tmp)


def amp_info(fps_total, auto_total):
    """

    输出AmpStruct结构体数组

    """

    sig_info = []
    # 提前开辟数组空间
    for i in range(len(fps_total[0])):
        sig_struct = AmpStruct(i)
        sig_info.append(sig_struct)

    # 计算ampstruct结构体
    for i in range(len(fps_total)):
        for j in range(len(fps_total[0])):
            if not fps_total[i][j] in sig_info[j].amp_dict.keys():
                sig_info[j].amp_dict.update({fps_total[i][j]: 1})
                # print(sig_info[j].amp_dict)
            else:
                sig_info[j].amp_dict[fps_total[i][j]] += 1
            if fps_total[i][j] > auto_total[i][j]:
                sig_info[j].occupancy += 1

    # 计算幅度均值
    auto_avg = np.zeros(len(auto_total[0]))
    for auto_list in auto_total:
        auto_avg += auto_list
    auto_avg /= len(auto_total)

    # 幅度均值放入ampstruct结构体
    for i in range(len(sig_info)):
        sig_info[i].threshold_avg = int(auto_avg[i])

    return sig_info


# def freq_avg(file, avg_count):
#     """
#
#     传入avg_count帧数据，返回平均值
#
#     """
#
#     np_data_total = []
#     counter = avg_count
#     for frame in file:
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


def freq_avg(frame_list, avg_count):
    """

    传入avg_count帧数据，返回平均值

    """

    np_data_total = []
    counter = avg_count

    # 合并被拆分的帧结构
    frame_list_combined = []
    while frame_list:
        current_frame = frame_list.pop(0)

        if not current_frame[1]:
            continue

        if current_frame[1][8] == current_frame[1][3]:
            frame_list_combined = frame_list
            break

        if not frame_list:
            break

        current_channel_count = current_frame[1][8]
        while current_channel_count < current_frame[1][3]:
            if not frame_list:
                break
            next_frame = frame_list.pop(0)
            current_channel_count += next_frame[1][8]
            tmp = combine_frame(current_frame, next_frame)
            current_frame = tmp
        frame_list_combined.append(tmp)

    for frame in frame_list_combined:
        if not frame[1]:
            continue

        fp_data = np.array(list(map(lambda x: float(x) / 10, frame[1][-1])))
        np_data_total.append(fp_data)

        counter -= 1
        if not counter:

            tmp = np.zeros(shape=(1, len(np_data_total[0])))
            for i in np_data_total:
                tmp += i

            fp_data = (tmp/avg_count).round(1)

            counter = avg_count
            np_data_total = []

            frame[1][-1] = fp_data[0]
            yield frame


def get_businessid(start_freq, stop_freq):
    """

    通过起始结束频率查询表获取监测业务编号 单位 hz

    """
    cursor = hc.get_hive_cursor('172.18.140.8', 'rmdsd')
    sql = 'select servicedid from rmbt_service_freqdetail where startfreq={0} and endfreq={1}'.format(
        start_freq, stop_freq
    )
    res = hc.execute_sql(cursor, sql)
    # flag: 表中是否有当前业务编号
    flag = True
    # 若表中无对应数据，生成自定义频段
    if not res:
        res = uuid.uuid1()
        flag = False
        # sql = 'insert into table rmbt_service_freqdetail ' \
        #       'values ("{0}","00000000-0000-0000-0000-000000000000","{1}-{2}Mhz",{1},{2},25.0)'.format(
        #        res, start_freq, stop_freq)
        # hc.execute_sql_insert(cursor, sql)

    return res, flag


def file_resolve(file, mfid, start_freq, stop_freq, file_size_min, data_type):
    """

    生成信号分选结构及频谱数据按分钟中间值写入文件

    """
    print('file resolving...')
    frame_count = 0
    fp_data_total = []
    auto_total = []
    sb_list = []

    time_tmp = time.localtime(time.mktime(time.strptime(next(Read(file).header_payload())[0][3], '%Y-%m-%d %H:%M:%S.%f')))
    time_tmp = time_tmp.tm_min

    for frame_tmp in Read(file).header_payload():
        if not frame_tmp[1]:
            continue
        frame = frame_tmp
        break

    # start_freq = frame[1][4]/1000
    # stop_freq = frame[1][5]/1000
    step = frame[1][7]

    # 通过起始结束频率查询监测业务编号
    bid_tmp, flag = get_businessid(start_freq, stop_freq)
    businessid = bid_tmp[0][0] if isinstance(bid_tmp, list) else bid_tmp

    frame_list = []
    for i in Read(file).header_payload():
        frame_list.append(i)
    for frame in freq_avg(frame_list, 10):
        # break
        if not frame[1]:
            continue
        frame_count += 1

        time_str = frame[0][3]
        time_ts = time.mktime(time.strptime(frame[0][3], '%Y-%m-%d %H:%M:%S.%f'))
        time_struct = time.localtime(time_ts)

        if time_struct.tm_min == time_tmp:

            fp_data = frame[1][-1]

            start_freq = frame[1][4]/1000
            stop_freq = frame[1][5]/1000
            step = frame[1][7]/1000

            so = CInvoker(fp_data, start_freq, stop_freq, step)
            # 信号分选
            sig_count, cf, cfi, cfa, snr, sb = so.signal_detect()

            if sig_count > 0:
                # 自动门限
                auto = so.auto_threshold()
                auto_np = np.array(auto)
                fp_np = np.array(fp_data)
                auto_total.append(auto_np)
                fp_data_total.append(fp_np)
                # add func - find max of signalband
                sb_list.append(sb)

                sigDetectResult = np.array([cf, cfi, cfa, snr, sb])
                # 将信号写入文件
                try:
                    signal_to_csv(mfid, frame[0][3], sig_count, sigDetectResult)
                except TypeError:
                    print('no signal detected')
            else:
                continue

        else:
            amp_struct_info = amp_info(fp_data_total, auto_total)
            # add func - find max of sigbalband
            sb_array = np.array(sb_list)
            print(sb_array)
            sb_max = sb_array.max()
            print(sb_max)

            print('总扫描帧数量 {0}'.format(frame_count))

            fp_data_total = []
            auto_total = []
            frame_count = 0
            time_tmp = time_struct.tm_min

            # 幅度值字典写入文件
            for amp_struct in amp_struct_info:
                scan_count = 0
                for j in amp_struct.amp_dict.values():
                    scan_count += j

                with open('/var/dropzone/amp_info.min.tmp', 'a') as f:
                    f.write('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}|{12}|{13}'.format(
                        businessid, mfid, time_str.split('.')[0], 4, amp_struct.sig_index, start_freq, stop_freq, step,
                        amp_struct.amp_dict, amp_struct.occupancy, scan_count, amp_struct.threshold_avg, file_size_min, data_type
                    ))
                    f.write('\n')

    # 数据库中没有当前business_id则插入
    if not flag:
        cursor = hc.get_hive_cursor('172.18.140.8', 'rmdsd')
        sql = 'insert into table rmbt_service_freqdetail ' \
              'values ("{0}","00000000-0000-0000-0000-000000000000","{1}-{2}Mhz",{1},{2},25.0)'.format(
               businessid, start_freq, stop_freq)
        hc.execute_sql_insert(cursor, sql)

    try:
        shutil.move('/var/dropzone/amp_info.min.tmp', '/var/dropzone/amp_info.min')
        shutil.move('/var/dropzone/signal.tmp', '/var/dropzone/signal')
    except shutil.Error as msg:
        print('file_resolve error---'+msg)

    print('file resolved...')


def combine_frame(*args):
    """ 例如 87-108 被拆分为 87-106,106-108 两段或任意段，合并 """

    levels = []
    start_freq = args[0][1][4]
    pl = 0
    dl = 0
    channel_total = 0
    for i in range(len(args)):
        pl += args[i][0][4]
        dl += args[i][1][1]
        channel_total += args[i][1][8]
        stop_freq = args[i][1][5]
        levels.extend(args[i][1][-1])
    head = (args[0][0][0], args[0][0][1], args[0][0][2], args[-1][0][3], pl, args[0][0][5])
    payload = [args[0][1][0], dl, args[0][1][2], args[0][1][3], start_freq, stop_freq, args[0][1][6], args[0][1][7], channel_total, levels]

    result = (head, payload)
    return result
