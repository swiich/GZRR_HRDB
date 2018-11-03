# coding=utf-8

from struct import unpack
import os
from tools.c_lib.c_invoker import CInvoker
import numpy as np
from math import radians, cos, sin, asin, sqrt
from tools import traverse_file
import time
import uuid
import csv

csv_file = './station.csv'


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
        # 将台站库存入内存，避免多次查询
        station = []
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for i in reader:
                try:
                    row = (i[5], float(i[-3]), float(i[-4]), float(i[-1]), i[-7], float(i[-5]))
                except Exception as e:
                    pass
                station.append(row)
        station = tuple(station)

        first_frame = next(self.resolve())
        code = str(first_frame[0][0]) + '00' + str(first_frame[0][1])
        time_str = time.strptime(first_frame[0][10], '%Y-%m-%d %H:%M:%S.%f')
        date = str(time_str.tm_year)+'-'+str(time_str.tm_mon)+'-'+str(time_str.tm_mday)
        longitude = first_frame[0][-4]
        latitude = first_frame[0][-3]
        data_len = first_frame[0][-1]
        tmp_ocy = np.zeros(data_len)
        tmp_fpdata = np.zeros(data_len)
        scan_count = 0
        for i in self.resolve():
            stop_freq = i[0][3]
            start_freq = i[0][2]
            step = i[0][4]
            fp_data = i[1]
            tmp_fpdata += np.array(fp_data)

            occupancy = ocy(fp_data, start_freq, stop_freq, step)
            tmp_ocy += occupancy
            scan_count += 1
            print(scan_count)

        tmp_fpdata = tmp_fpdata/scan_count
        index = freq_band_index_split(int(start_freq), int(stop_freq), int(step))
        point = dict(zip(index/1000000, tmp_fpdata))
        t = dict(zip(index/1000000, tmp_ocy.astype(np.int32)))
        # 匹配台站
        freqregion_total = []
        for j in station:
            freqregion_start = float(j[-2].split('-')[0])*1000000
            freqregion_stop = float(j[-2].split('-')[1])*1000000
            # 监测车在台站发射频率范围内并且台站发射频段在监测频段范围内，为有效台站
            if haversine(longitude / 100000000, latitude / 100000000, j[1], j[2]) < j[3] and \
                    (self.start_freq < freqregion_start < self.stop_freq or
                     self.start_freq < freqregion_stop < self.stop_freq):
                guid = uuid.uuid1()
                freqregion = j[-2]
                staid = j[0]
                activepoint = freq_band_split(t, freqregion_start/1000000, freqregion_stop/1000000)
                with open('facility', 'a') as f:
                    f.write(code+'|'+str(guid)+'|'+freqregion+'|'+staid+'|'+date+'|'+str(scan_count)+'|'+str(activepoint))
                    f.write('\n')
                # 判断违规频点
                station_power = float(j[-1])
                for item in point.items():
                    if item[1] > station_power:
                        illegal_freq = item[0]
                        illegal_type = '0'
                        mfid = code
                        with open('illegal_freq', 'a') as f:
                            f.write(str(illegal_freq)+','+illegal_type+','+mfid+','+date+','+staid)
                            f.write('\n')

                freqregion_total.append(freqregion)

        # 判断违规频率, 频点不在监测范围内，并且频点占用度超过3%
        if freqregion_total:

            section = section_cut(str(self.start_freq/1000000)+'-'+str(self.stop_freq/1000000), self.step, *freqregion_total)
            for i in section:
                if t[i]/scan_count > 0.03:
                    illegal_freq = i
                    illegal_type = '1'
                    mfid = code
                    with open('illegal_freq', 'a') as f:
                        f.write(str(illegal_freq) + ',' + illegal_type + ',' + mfid + ',' + date + ',' + '')
                        f.write('\n')

        return 1

    def monitoring_car_data_min(self):
        """
        将监测车数据每分钟合并为一帧
        """
        # 将台站库存入内存，避免多次查询
        station = []
        with open(csv_file, 'r') as f:
            reader = csv.reader(f)
            for i in reader:
                try:
                    row = (i[5], float(i[-3]), float(i[-4]), float(i[-1]), i[-7], float(i[-5]))
                except Exception as e:
                    pass
                station.append(row)
        station = tuple(station)

        scan_count = 0
        first_frame = next(self.resolve())
        start_freq = first_frame[0][2]
        stop_freq = first_frame[0][3]
        step = first_frame[0][4]
        code = str(first_frame[0][0]) + '00' + str(first_frame[0][1])
        fp_data_total = np.zeros(first_frame[0][-1])
        cvg_data = np.zeros(first_frame[0][-1])
        time_min = time.strptime(first_frame[0][10], '%Y-%m-%d %H:%M:%S.%f').tm_min
        time_str = time.strptime(first_frame[0][10], '%Y-%m-%d %H:%M:%S.%f')
        date = str(time_str.tm_year)+'-'+str(time_str.tm_mon)+'-'+str(time_str.tm_mday)

        # res = []
        for i in self.resolve():
            time_current = time.strptime(i[0][10], '%Y-%m-%d %H:%M:%S.%f')

            # 合并一分钟监测车数据为一帧
            if time_current.tm_min == time_min:
                fp_data_total += i[1]
                cvg_data += ocy(i[1], i[0][2], i[0][3], i[0][4])
                scan_count += 1
            else:
                fp_data_min = (fp_data_total / scan_count).round()
                index = freq_band_index_split(int(start_freq), int(stop_freq), int(step))
                point = dict(zip(index / 1000000, fp_data_min))
                # time_str = i[0][10]
                longitude = i[0][-4]
                latitude = i[0][-3]
                col, row = coordinate(longitude, latitude)
                # 将每帧数据经纬度与台站数据库比对
                freqregion_total = []
                for j in station:
                    freqregion_start = float(j[-2].split('-')[0])*1000000
                    freqregion_stop = float(j[-2].split('-')[1])*1000000
                    if haversine(longitude/100000000, latitude/100000000, j[1], j[2]) < j[3] and \
                                self.start_freq < freqregion_start < self.stop_freq or self.start_freq < freqregion_stop < self.stop_freq:
                        staid = j[0]
                        freqregion = j[-2]
                        cvg = round(sum(cvg_data)/(scan_count*first_frame[0][-1])*100, 2)
                        ocy_data = ocy(fp_data_min, i[0][2], i[0][3], i[0][4])
                        ocy_res = round(sum(ocy_data)/ocy_data.size * 100, 2)
                        with open('car', 'a') as f:
                            f.write(code+','+str(col)+','+str(row)+','+j[4]+','+j[5]+','+str(ocy_res)+','+str(cvg))
                            f.write('\n')
                        # 判断违规频点
                        station_power = float(j[-1])
                        for item in point.items():
                            if item[1] > station_power:
                                illegal_freq = item[0]
                                illegal_type = '0'
                                mfid = code
                                with open('illegal_freq', 'a') as f:
                                    f.write(
                                        str(illegal_freq) + ',' + illegal_type + ',' + mfid + ',' + date + ',' + staid)
                                    f.write('\n')

                        freqregion_total.append(freqregion)
                # 判断违规频率, 频点不在监测范围内，并且频点占用度超过3%
                if freqregion_total:
                    print('计算违规频率。。。')
                    section = section_cut(str(self.start_freq / 1000000) + str(self.stop_freq / 1000000), self.step,
                                          *freqregion_total)
                    index = freq_band_index_split(int(start_freq), int(stop_freq), int(step))
                    t = dict(zip(index / 1000000, cvg_data.astype(np.int32)))
                    for i in section:
                        if t[i] / scan_count > 0.03:
                            illegal_freq = i
                            illegal_type = '1'
                            mfid = code
                            with open('illegal_freq', 'a') as f:
                                f.write(str(illegal_freq) + ',' + illegal_type + ',' + mfid + ',' + date + ',' + '')
                                f.write('\n')

                fp_data_total = np.zeros(first_frame[0][-1])
                cvg_data = np.zeros(first_frame[0][-1])
                time_min = time_current.tm_min
                scan_count = 0

        return 1

    def calc(self):
        """
        判断文件为监测车或固定站并调用对应方法
        """
        # try:
        #     if self.flag == 'F':
        #         self.monitoring_facility_data_file()
        #     else:
        #         self.monitoring_car_data_min()
        #     print(self.file.name)
        #
        # except Exception as e:
        #     print(e)
        if self.flag == 'F':
            self.monitoring_facility_data_file()
        else:
            self.monitoring_car_data_min()
        print(self.file.name)


def freq_band_index_split(start_freq, stop_freq, step):
    """
    通过开始频率和结束频率以及步进返回拆分后频点列表
    单位统一为   hz
    """
    if (stop_freq-start_freq) % step == 0:
        res = [i for i in range(int(start_freq), int(stop_freq), int(step))]
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


# def match_stationid(longitude, latitude):
#     """
#     通过经纬度匹配频段区域，台站id
#     """
#     cursor = hc.get_hive_cursor('172.18.140.8', 'spectrum_evaluation')
#     sql = "select stat_lg,stat_la,st_serv_r,freqregion,staid from station"
#     res = hc.execute_sql(cursor, sql)
#     for i in res:
#         if haversine(longitude, latitude, i[0], i[1]) < i[2]:
#             yield i[3], i[4]


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


def section_cut(freq_band, step, *args):
    """
    freq_band为监测频段，args为台站工作频段，函数返回数组，值为台站工作频段与监测频段的差集频点
    freq_band, args为字符串，格式为 '000-000'
    step单位   hz
    """
    tmp = freq_band_index_split(float(freq_band.split('-')[0])*1000000, float(freq_band.split('-')[1])*1000000, step)
    freq_band_list = list(tmp)
    try:
        for i in args:
            start_freq = float(i.split('-')[0]) * 1000000
            stop_freq = float(i.split('-')[1]) * 1000000
            for j in tmp:
                if start_freq <= j <= stop_freq:
                    freq_band_list.remove(j)
    except Exception as e:
        print(e, j)

    return np.array(freq_band_list)/1000000


if __name__ == '__main__':

    files = traverse_file.get_all_file('/storage/sdb/data', 'bin')
    count = 0
    for i in files:
        name = os.path.basename(i)
        if len(name.split('_')) == 9:
            SpectrumStatistics(i).calc()
            count += 1
            print(count)

