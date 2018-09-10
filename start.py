from socket_d.py_hdfs.py_hdfs import download_file
import os
from result_table_scripts.c_lib.c_invoker import CInvoker
from result_table_scripts.c_lib.signal_handler import amp_info, signal_to_csv, freq_avg
import time
import numpy as np


if __name__ == '__main__':
    # TODO: 将固定文件改为监控文件变动      统计监测数据基础表代码      amp_info中加上起始频率，步长，幅度门限均值   监测频点内容
    file = '11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.FSCAN'
    # file = '11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180808090648.0809.FSCAN'
    if not os.path.exists(file):
        file = download_file('/data/fscan/%s' % file, file)

    task_start_time = time.time()

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
                # signal_to_csv(mfid, frame[0][3], sig_count, sigDetectResult)

            else:
                print(sig_count)
            print(time_str)

        else:
            amp_struct_info = amp_info(fp_data_total, auto_total)
            print('总扫描帧数量 {0}'.format(frame_count))
            print(time_str)

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
                    f.write('{0}|{1}|{2}|{3}|{4}|{5}|{6}'.format(
                        mfid, time_str.split('.')[0], 4, amp_struct.sig_index,
                        amp_struct.amp_dict, amp_struct.occupancy, scan_count)
                            )
                    f.write('\n')

    print('任务总耗时 {0}'.format(time.time() - task_start_time))
