import numpy as np
from tools.analyse_stream import Read
import csv


class AmpStruct:
    def __init__(self, index=0):
        """

        sig_index: 频点索引号
        amp_dict: 幅度值字典
        occupancy: 频点占用度

        """
        self.sig_index = index
        self.amp_dict = {}
        self.occupancy = 0


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


def signal_to_csv(time, sig_count, sigdetectres):
    """

    将调用clib后信息写入文件

    """
    res = single_signal_return(sig_count, sigdetectres)
    with open('signal.csv', 'a') as f:
        w = csv.writer(f)
        for i in res:
            tmp = ('11000001111111', time, *i)
            w.writerow(tmp)


def amp_info(fps_total, auto_total):
    """

    传入 fps_total [[频点数组1] ... [频点数组n]], 长度为 frame_total, auto_total为门限，同理
    data_len为一帧频点数量
    输出AmpStruct结构体数组

    """

    print('calculating...')
    # 提前开辟数组空间
    sig_info = []
    for i in range(len(fps_total[0])):
        sig_struct = AmpStruct(i)
        sig_info.append(sig_struct)

    for i in range(len(fps_total)):
        for j in range(len(fps_total[0])):
            if not fps_total[i][j] in sig_info[j].amp_dict.keys():
                sig_info[j].amp_dict.update({fps_total[i][j]: 1})
            else:
                sig_info[j].amp_dict[fps_total[i][j]] += 1
            if fps_total[i][j] > auto_total[i][j]:
                sig_info[j].occupancy += 1
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

            tmp = np.zeros(shape=(1,len(np_data_total[0])))
            for i in np_data_total:
                tmp += i

            fp_data = (tmp/avg_count).round(1)

            counter = avg_count
            np_data_total = []

            frame[1][0][9] = fp_data[0]
            yield frame
