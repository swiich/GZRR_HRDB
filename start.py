from socket_d.py_hdfs.py_hdfs import download_file
import os
from tools.c_lib.c_invoker import CInvoker
from tools.signal_handler import *
import time
import numpy as np
from tools.file_info import file_index, des_save
from tools.MyThread import MyThread


def file_resolve(file):
    """

    生成信号分选结构及频谱数据按分钟中间值写入文件

    """
    print('file resolving...')
    frame_count = 0
    fp_data_total = []
    auto_total = []

    time_tmp = time.localtime(time.mktime(time.strptime(next(freq_avg(file, 10))[0][3], '%Y-%m-%d %H:%M:%S.%f')))
    time_tmp = time_tmp.tm_min

    mfid = file.split('-')[0]
    frame = next(freq_avg(file, 10))
    start_freq = frame[1][0][4]/1000
    stop_freq = frame[1][0][5]/1000
    step = frame[1][0][7]
    # 通过起始结束频率查询监测业务编号
    bid_tmp = get_businessid(start_freq, stop_freq)
    businessid = bid_tmp[0][0] if isinstance(bid_tmp, list) else bid_tmp

    for frame in freq_avg(file, 10):
        frame_count += 1

        time_str = frame[0][3]
        time_ts = time.mktime(time.strptime(frame[0][3], '%Y-%m-%d %H:%M:%S.%f'))
        time_struct = time.localtime(time_ts)

        if time_struct.tm_min == time_tmp:

            fp_data = frame[1][0][-1]

            start_freq = frame[1][0][4]/1000
            stop_freq = frame[1][0][5]/1000
            step = frame[1][0][7]/1000

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

                sigDetectResult = np.array([cf, cfi, cfa, snr, sb])
                # 将信号写入文件
                signal_to_csv(mfid, frame[0][3], sig_count, sigDetectResult)

            else:
                print(sig_count)

        else:
            amp_struct_info = amp_info(fp_data_total, auto_total)
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

                with open('amp_info.min', 'a') as f:
                    f.write('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}'.format(
                        businessid, mfid, time_str.split('.')[0], 4, amp_struct.sig_index, start_freq, stop_freq, step,
                        amp_struct.amp_dict, amp_struct.occupancy, scan_count, amp_struct.threshold_avg
                    ))
                    f.write('\n')
    print('file resolved...')


if __name__ == '__main__':
    # TODO: 将固定文件改为监控文件变动, 自动化
    # TODO: 移动车经纬度文件
    starttime = time.time()
    file_data = '52010001119001-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.FSCAN'
    file_des = '52010001119001-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.des'
    # file = '11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180808090648.0809.FSCAN'
    if not os.path.exists(file_data):
        file_data = download_file('/data/fscan&spectrum/%s' % file_data, file_data)
        print(file_data)

    t1 = MyThread(func=file_resolve, args=(file_data,))
    t2 = MyThread(file_index, (file_data, file_des, 'file_index'))
    t3 = MyThread(file_index, (file_data, file_des, 'task_info'))
    t4 = MyThread(file_index, (file_data, file_des, 'device_info'))
    t5 = MyThread(des_save, (file_des,))

    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()

    res = [t1, t2, t3, t4, t5]
    for t in res:
        t.join()
        print(t.get_result())

    stoptime = time.time()
    print(stoptime-starttime)
