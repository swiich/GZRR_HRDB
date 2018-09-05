from tools.analyse_stream import Read
from socket_d.py_hdfs.py_hdfs import download_file
import os
from result_table_scripts.c_lib.c_invoker import CInvoker
from result_table_scripts.c_lib.signal_handler import *
import csv
import time


if __name__ == '__main__':
    file = 'test.bin'
    if not os.path.exists(file):
        file = download_file('/data/fscan/11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180808090648.0809.FSCAN', file)
        # file = download_file('/data/fscan/11000001111111-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.FSCAN', file)

    start = time.time()

    counter = 800
    f_len = counter
    fp_data_total = []
    auto_total = []
    for frame in Read(file, 'fsc').header_payload():

        fp_data = list(map(lambda x: float(x) / 10, frame[1][0][-1]))

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
            counter -= 1

            sigDetectResult = np.array([cf, cfi, cfa, snr, sb])
            # 将信号写入文件
            signal_to_csv(frame[0][3], sig_count, sigDetectResult)

        else:
            print(sig_count)
        if counter == 0:
            break

    # a = amp_info(fp_data_total, auto_total)

    end = time.time()
    # print(a[5].sig_index,a[5].amp_dict,a[5].occupancy)
    print(end - start)
