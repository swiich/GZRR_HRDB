import numpy as np
from tools.analyse_stream import Read
import csv
import uuid
from socket_d.hive import hive_connector as hc


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


def signal_to_csv(mfid, time, sig_count, sigdetectres):
    """

    将调用clib后信息写入文件

    """
    res = single_signal_return(sig_count, sigdetectres)
    with open('signal', 'a') as f:
        w = csv.writer(f)
        for i in res:
            tmp = (mfid, time, *i)
            w.writerow(tmp)


def amp_info(fps_total, auto_total):
    """

    输出AmpStruct结构体数组

    """

    print('calculating...')
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
    print('calculated...')

    return sig_info


def freq_avg(file, avg_count):
    """

    传入avg_count帧数据，返回平均值

    """

    np_data_total = []
    counter = avg_count
    for frame in Read(file, 'fsc').header_payload():

        fp_data = np.array(list(map(lambda x: float(x) / 10, frame[1][0][-1])))
        np_data_total.append(fp_data)

        counter -= 1
        if not counter:

            tmp = np.zeros(shape=(1, len(np_data_total[0])))
            for i in np_data_total:
                tmp += i

            fp_data = (tmp/avg_count).round(1)

            counter = avg_count
            np_data_total = []

            frame[1][0][9] = fp_data[0]
            yield frame


def get_businessid(start_freq, stop_freq):
    """

    通过起始结束频率查询表获取监测业务编号

    """
    cursor = hc.get_hive_cursor('172.18.140.8', 'rmdsd')
    sql = 'select servicedid from rmbt_service_freqdetail where startfreq=={0} and endfreq = {1}'.format(
        start_freq, stop_freq
    )
    res = hc.execute_sql(cursor, sql)
    # 若表中无对应数据，生成自定义频段
    if not res:
        res = uuid.uuid1()
        sql = 'insert into table rmbt_service_freqdetail ' \
              'values ("{0}","","自定义频段",{1},{2},"NULL")'.format(res, start_freq, stop_freq)
        hc.execute_sql_insert(cursor, sql)

    return res
