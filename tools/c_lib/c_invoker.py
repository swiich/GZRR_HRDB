from ctypes import *

so_auto = CDLL('./result_table_scripts/c_lib/libAutoThreshold.so')
so_sigdetect = CDLL('./result_table_scripts/c_lib/libSignalDetect.so')


class CInvoker:
    """
    传入参数类型为Python类型
    start_freq, stop_freq, step 单位均为 KHz
    fp_data: 频点数组
    """
    def __init__(self, fp_data, start_freq, stop_freq, step):

        self.fp_data_len = c_int(len(fp_data))
        self.fp_data_c = (c_float * self.fp_data_len.value)(*fp_data)
        self.start_freq = c_double(start_freq)
        self.stop_freq = c_double(stop_freq)
        self.step = c_float(step)

    def auto_threshold(self):
        # (ctype * length)声明指针？类型？后，需要实例化
        auto = (c_float*self.fp_data_len.value)()

        so_auto.AutothresholdFun.argtypes = [POINTER(c_float), c_int, c_double, c_double, c_float, POINTER(c_float)]
        so_auto.AutothresholdFun(self.fp_data_c, self.fp_data_len, self.start_freq, self.stop_freq, self.step, auto)
        return auto

    def signal_detect(self):
        smooth_frame = c_int(10)
        freq_band = (str(int((self.start_freq.value / 1000))) + '-' + str(int((self.stop_freq.value / 1000)))).encode()

        # 声明传入参数类型
        so_sigdetect.SigDetectFun.argtypes = [
            POINTER(c_float), c_int, c_double, c_double, c_float, c_int, c_char_p
        ]
        so_sigdetect.retSigDetectResult.argtypes = [
            c_int, POINTER(c_float), POINTER(c_int), POINTER(c_float), POINTER(c_float), POINTER(c_float)
        ]

        sig_count = so_sigdetect.SigDetectFun(self.fp_data_c, self.fp_data_len, self.start_freq,
                                              self.stop_freq, self.step, smooth_frame, freq_band)
        if sig_count > 0:
            center_freq = (c_float*sig_count)()
            center_freq_index = (c_int*sig_count)()
            center_freq_amp = (c_float*sig_count)()
            snr = (c_float*sig_count)()
            signal_band = (c_float*sig_count)()
            # 获取信号分选算法返回数据
            so_sigdetect.retSigDetectResult(
                c_int(sig_count), center_freq, center_freq_index, center_freq_amp, snr, signal_band
            )

            return sig_count, center_freq, center_freq_index, center_freq_amp, snr, signal_band

        else:
            return
