# coding=utf-8

from struct import unpack
import os
from tools.c_lib.c_invoker import CInvoker
import numpy as np
from tools import traverse_file
import time


class SpectrumStatistics:
    def __init__(self, file):
        f_name = os.path.basename(file).split('_')
        self.file = open(file, 'rb')
        self.mscode = f_name[0]
        self.mfcode = f_name[1]
        # self.file_create_time = f_name[2] + f_name[3]
        self.start_freq = float(f_name[4].split('M')[0])*1000000
        self.stop_freq = float(f_name[5].split('M')[0])*1000000
        self.step = float(f_name[6].split('k')[0])*1000
        self.p_mode = f_name[7]
        self.flag = f_name[-1][0]

    def resolve(self):
        file = self.file
        while True:
            leader = file.read(4).hex()
            if not leader:
                break
            equid = unpack('2b', file.read(2))
            device_inf, = unpack('b', file.read(1))
            time = unpack('<H5BH', file.read(9))
            scan_rate, = unpack('h', file.read(2))
            longitude, latitude = unpack('2q', file.read(16))
            antenna_height, = unpack('h', file.read(2))
            # start_freq, = unpack('d', file.read(8))
            # step, = unpack('f', file.read(4))
            file.read(12)
            fp_count, = unpack('i', file.read(4))

            time_str = "{0}-{1}-{2} {3}:{4}:{5}.{6}".format(*time)
            data = []
            for i in range(fp_count):
                data.append(unpack('h', file.read(2))[0])

            res = (self.mscode, self.mfcode, self.start_freq, self.stop_freq,
                   self.step, self.p_mode, self.flag, leader, str(equid[0])+str(equid[1]),
                   device_inf, time_str, scan_rate, longitude, latitude, antenna_height,
                   fp_count)
            yield res, data
        file.close()

    def write_to_file(self, des_path):
        """ 解析结果存入数据文件 """
        file_res = open(des_path, 'a')
        for h, d in self.resolve():
            for i in h:
                file_res.write(str(i)+'|')
            for i, v in enumerate(d):
                if i != len(d)-1:
                    file_res.write(str(v)+',')
                else:
                    file_res.write(str(v))
            file_res.write('\n')
            # break
        file_res.close()
        print(self.file.name)


def freq_band_split(start_freq, stop_freq, step):
    """
    通过开始频率和结束频率以及步进返回拆分后列表
    单位统一为   hz
    """
    if (stop_freq-start_freq) % step == 0:
        res = [i for i in range(start_freq, stop_freq, step)]
        res.append(stop_freq)
    else:
        raise ValueError

    return res


if __name__ == '__main__':
    # starttime = time.time()
    #
    # file_ = traverse_file.get_all_file('data/s1')
    # for f in file_:
    #     SpectrumStatistics(f).write_to_file('data/s1/res')
    #
    # file_ = traverse_file.get_all_file('data/s2')
    # for f in file_:
    #     SpectrumStatistics(f).write_to_file('data/s2/res')
    #
    # file_ = traverse_file.get_all_file('data/s3')
    # for f in file_:
    #     SpectrumStatistics(f).write_to_file('data/s3/res')
    #
    # file_ = traverse_file.get_all_file('data/s4')
    # for f in file_:
    #     SpectrumStatistics(f).write_to_file('data/s4/res')
    #
    # file_ = traverse_file.get_all_file('data/s5')
    # for f in file_:
    #     SpectrumStatistics(f).write_to_file('data/s5/res')
    #
    # print('耗时: ', time.time()-starttime)

    file = '/home/data/s1/52010000_0002_20180809_171213_780MHz_980MHz_12.5kHz_V_F.bin'
    a = next(SpectrumStatistics(file).resolve())
    stop_freq = a[0][3] / 1000
    start_freq = a[0][2] / 1000
    step = a[0][4] / 1000
    fp_data = a[1]

    so = CInvoker(fp_data, start_freq, stop_freq, step)
    auto = so.auto_threshold()
    np_fp_data = np.array(fp_data)
    np_auto = np.array(auto)
    print(np_fp_data > np_auto)

