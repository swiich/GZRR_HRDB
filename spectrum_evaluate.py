# coding=utf-8

from struct import unpack
import os
from tools.c_lib.c_invoker import CInvoker
import numpy as np
from math import radians, cos, sin, asin, sqrt
from tools import traverse_file
import time
import hive_connector as hc


class SpectrumStatistics:
    """
    解析频谱评估数据文件
    """
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

    def monitoring_facility_data_file(self):
        """
        将固定站数据每个文件合并为一帧
        """
        data_len = next(self.resolve())[0][-1]
        tmp = np.zeros(data_len)
        scan_count = 0
        for i in self.resolve():
            stop_freq = i[0][3]
            start_freq = i[0][2]
            step = i[0][4]
            fp_data = i[1]

            occupancy = ocy(fp_data, start_freq, stop_freq, step)
            tmp += occupancy
            scan_count += 1
            print(scan_count)
        index = freq_band_index_split(int(start_freq), int(stop_freq), int(step))
        t = dict(zip(index/1000000, tmp))
        # TODO: 指定返回频段范围
        a = freq_band_split(t, 880, 890)
        print(a)

    def monitoring_car_data_min(self):
        """
        将监测车数据每分钟合并为一帧
        """
        # 将台站库存入内存，避免多次查询数据库
        cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
        sql = "select stat_lg,stat_la,st_serv_r,freqregion,staid from station"
        station = hc.execute_sql(cursor, sql)

        scan_count = 0
        first_frame = next(self.resolve())
        fp_data_total = np.zeros(first_frame[0][-1])
        time_min = time.strptime(first_frame[0][10], '%Y-%m-%d %H:%M:%S.%f').tm_min
        for i in self.resolve():
            # 合并一分钟监测车数据为一帧
            time_current = time.strptime(i[0][10], '%Y-%m-%d %H:%M:%S.%f')

            if time_current.tm_min == time_min:
                fp_data_total += i[1]
                scan_count += 1
            else:
                print(time_min)
                fp_data_min = (fp_data_total / scan_count).round()
                time_str = i[0][10]
                longitude = i[0][-4]
                latitude = i[0][-3]
                col, row = coordinate(longitude, latitude)
                # print(longitude,latitude)
                for i in station:
                    if haversine(longitude, latitude, i[0], i[1]) < i[2]:
                        print(ocy(fp_data_min, i[0][2], i[0][3]))

                fp_data_total = np.zeros(first_frame[0][-1])
                time_min = time_current.tm_min
                scan_count = 0


def freq_band_index_split(start_freq, stop_freq, step):
    """
    通过开始频率和结束频率以及步进返回拆分后列表
    单位统一为   hz
    """
    if (stop_freq-start_freq) % step == 0:
        res = [i for i in range(start_freq, stop_freq, step)]
        res.append(stop_freq)
    else:
        raise ValueError

    return np.array(res)


def freq_band_split(d, start_freq, stop_freq):
    """
    返回指定范围内的字典元素
    """
    res = {key: value for key, value in d.items() if start_freq <= key <= stop_freq}

    return res


def ocy(fp_data, start_freq, stop_freq, step):
    """
    计算每一帧频点是否超过门限，返回bool数组
    单位  hz
    """
    so = CInvoker(fp_data, start_freq / 1000, stop_freq / 1000, step / 1000)
    auto = so.auto_threshold()
    np_fp_data = np.array(fp_data)
    np_auto = np.array(auto)

    return np_fp_data > np_auto


def remove_null_from_dict(d):
    """
    去掉字典中超过门限次数为0的元素
    """
    res = d.copy()
    for item in d.items():
        if item[1] == 0:
            res.pop(item[0])

    return res


def match_stationid(longitude, latitude):
    """
    通过经纬度匹配频段区域，台站id
    """
    cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
    sql = "select stat_lg,stat_la,st_serv_r,freqregion,staid from station"
    res = hc.execute_sql(cursor, sql)
    for i in res:
        if haversine(longitude, latitude, i[0], i[1]) < i[2]:
            yield i[3], i[4]


def coordinate(longitude, latitude):
    """
    返回地图网格row,column
    """
    cell_size = 0.01
    lb_coord_x = 72
    lb_coord_y = 3

    col = int((longitude/100000000 - lb_coord_x)/cell_size)
    row = int((latitude/100000000 - lb_coord_y)/cell_size)

    return col, row


def haversine(lon1, lat1, lon2, lat2):
    """
    传入两点经纬度，计算两点之间球体距离
    返回单位 m
    """
    # 将十进制度数转化为弧度
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # Haversine公式
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6378.137  # 地球平均半径，单位为公里
    d = c * r * 1000

    return round(d, 3)


if __name__ == '__main__':
    pass
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


    # file = '/home/data/2018_gz_spectrumEvaluate/贵阳/固定监测/7月25日/52010000_0202_20180724_170338_780MHz_980MHz_12.5kHz_V_F.bin'
    file = '/home/data/2018_gz_spectrumEvaluate/修文、贵阳东站移动监测数据/10月23日移动监测/52010000_0001_20181023_122631_780MHz_980MHz_12.5kHz_V_M.bin'
    SpectrumStatistics(file).monitoring_car_data_min()
    # for i in match_stationid(10672102666/1000, 2656997000/1000):
    #     print(i)