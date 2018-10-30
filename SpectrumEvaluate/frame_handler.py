import numpy as np


file = '/home/data/s1/52010000_0002_20180809_171213_780MHz_980MHz_12.5kHz_V_F.bin'
# file = './data/spectrumstatics/52010000_0002_20180810_112233_780MHz_980MHz_12.5kHz_V_F.bin'
a = next(SpectrumStatistics(file).resolve())
stop_freq = a[0][3] / 1000
start_freq = a[0][2] / 1000
step = a[0][4] / 1000
fp_data = a[1]

so = CInvoker(fp_data, start_freq, stop_freq, step)
auto = so.auto_threshold()
# print(fp_data)
for i in auto:
    print(i)