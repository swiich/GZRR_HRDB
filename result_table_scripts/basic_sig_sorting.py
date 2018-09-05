# from hive_connector import signaldetect_input_args

spectrum_file = 'spectrum100799614.csv'
result_file = '../result-%s'%spectrum_file

mfid = '11000001111111'
s_a_t = '2018-08-01 15:29:50.170'

# sig_appear_time = eval(signaldetect_input_args(spectrum_file)[0][3])
with open(result_file) as f:
    result = eval(f.readline())

