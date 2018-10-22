import numpy as np
from tools.analyse_stream import Read
import csv
import uuid
import hive_connector as hc
import time
from tools.c_lib.c_invoker import CInvoker
import shutil


class AmpStruct:
    def __init__(self, index=0):
        """

        sig_index: 频点索引号
        amp_dict: 幅度值字典
        occupancy: 频点超过门限次数
        threshold_avg: 门限均值

        """
        self.sig_index = index
        self.amp_dict = {}
        self.occupancy = 0
        self.threshold_avg = 0


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


def signal_to_csv(mfid, time, sig_count, sigdetectres):
    """

    将调用clib后信息写入文件

    """
    res = single_signal_return(sig_count, sigdetectres)
    with open('/home/tmp/signal', 'a') as f:
        w = csv.writer(f)
        for i in res:
            tmp = (mfid, time, *i)
            w.writerow(tmp)
    shutil.move('/home/tmp/signal', '/home')


def amp_info(fps_total, auto_total):
    """

    输出AmpStruct结构体数组

    """

    sig_info = []
    # 提前开辟数组空间
    for i in range(len(fps_total[0])):
        sig_struct = AmpStruct(i)
        sig_info.append(sig_struct)

    # 计算ampstruct结构体
    for i in range(len(fps_total)):
        for j in range(len(fps_total[0])):
            if not fps_total[i][j] in sig_info[j].amp_dict.keys():
                sig_info[j].amp_dict.update({fps_total[i][j]: 1})
                # print(sig_info[j].amp_dict)
            else:
                sig_info[j].amp_dict[fps_total[i][j]] += 1
            if fps_total[i][j] > auto_total[i][j]:
                sig_info[j].occupancy += 1

    # 计算幅度均值
    auto_avg = np.zeros(len(auto_total[0]))
    for auto_list in auto_total:
        auto_avg += auto_list
    auto_avg /= len(auto_total)

    # 幅度均值放入ampstruct结构体
    for i in range(len(sig_info)):
        sig_info[i].threshold_avg = int(auto_avg[i])

    return sig_info


# def freq_avg(file, avg_count):
#     """
#
#     传入avg_count帧数据，返回平均值
#
#     """
#
#     np_data_total = []
#     counter = avg_count
#     for frame in file:
#         if not frame[1]:
#             continue
#
#         fp_data = np.array(list(map(lambda x: float(x) / 10, frame[1][-1])))
#         np_data_total.append(fp_data)
#
#         counter -= 1
#         if not counter:
#
#             tmp = np.zeros(shape=(1, len(np_data_total[0])))
#             for i in np_data_total:
#                 tmp += i
#
#             fp_data = (tmp/avg_count).round(1)
#
#             counter = avg_count
#             np_data_total = []
#
#             frame[1][-1] = fp_data[0]
#             yield frame


def freq_avg(frame_list, avg_count):
    """

    传入avg_count帧数据，返回平均值

    """

    np_data_total = []
    counter = avg_count

    # 合并被拆分的帧结构
    frame_list_combined = []
    while frame_list:
        current_frame = frame_list.pop(0)

        if not current_frame[1]:
            continue

        if current_frame[1][8] == current_frame[1][3]:
            frame_list_combined = frame_list
            break

        if not frame_list:
            break

        current_channel_count = current_frame[1][8]
        while current_channel_count < current_frame[1][3]:
            next_frame = frame_list.pop(0)
            current_channel_count += next_frame[1][8]
            tmp = combine_frame(current_frame, next_frame)
            current_frame = tmp
        frame_list_combined.append(tmp)

    for frame in frame_list_combined:
        if not frame[1]:
            continue

        fp_data = np.array(list(map(lambda x: float(x) / 10, frame[1][-1])))
        np_data_total.append(fp_data)

        counter -= 1
        if not counter:

            tmp = np.zeros(shape=(1, len(np_data_total[0])))
            for i in np_data_total:
                tmp += i

            fp_data = (tmp/avg_count).round(1)

            counter = avg_count
            np_data_total = []

            frame[1][-1] = fp_data[0]
            yield frame


def get_businessid(start_freq, stop_freq):
    """

    通过起始结束频率查询表获取监测业务编号 单位 hz

    """
    cursor = hc.get_hive_cursor('172.18.140.8', 'rmdsd')
    sql = 'select servicedid from rmbt_service_freqdetail where startfreq=={0} and endfreq = {1}'.format(
        start_freq, stop_freq
    )
    res = hc.execute_sql(cursor, sql)
    # 若表中无对应数据，生成自定义频段
    if not res:
        res = uuid.uuid1()
        sql = 'insert into table rmbt_service_freqdetail ' \
              'values ("{0}","00000000-0000-0000-0000-000000000000","{1}-{2}Mhz",{1},{2},25.0)'.format(
               res, start_freq, stop_freq)
        hc.execute_sql_insert(cursor, sql)

    return res


def file_resolve(file, mfid, start_freq, stop_freq):
    """

    生成信号分选结构及频谱数据按分钟中间值写入文件

    """
    print('file resolving...')
    frame_count = 0
    fp_data_total = []
    auto_total = []

    time_tmp = time.localtime(time.mktime(time.strptime(next(Read(file).header_payload())[0][3], '%Y-%m-%d %H:%M:%S.%f')))
    time_tmp = time_tmp.tm_min

    for frame_tmp in Read(file).header_payload():
        if not frame_tmp[1]:
            continue
        frame = frame_tmp
        break

    # start_freq = frame[1][4]/1000
    # stop_freq = frame[1][5]/1000
    step = frame[1][7]
    # 通过起始结束频率查询监测业务编号
    bid_tmp = get_businessid(start_freq, stop_freq)
    businessid = bid_tmp[0][0] if isinstance(bid_tmp, list) else bid_tmp

    frame_list = []
    for i in Read(file).header_payload():
        frame_list.append(i)
    for frame in freq_avg(frame_list, 10):
        # break
        if not frame[1]:
            continue
        frame_count += 1

        time_str = frame[0][3]
        time_ts = time.mktime(time.strptime(frame[0][3], '%Y-%m-%d %H:%M:%S.%f'))
        time_struct = time.localtime(time_ts)

        if time_struct.tm_min == time_tmp:

            fp_data = frame[1][-1]

            start_freq = frame[1][4]/1000
            stop_freq = frame[1][5]/1000
            step = frame[1][7]/1000

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

                with open('/home/tmp/amp_info.min', 'a') as f:
                    f.write('{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}|{9}|{10}|{11}'.format(
                        businessid, mfid, time_str.split('.')[0], 4, amp_struct.sig_index, start_freq, stop_freq, step,
                        amp_struct.amp_dict, amp_struct.occupancy, scan_count, amp_struct.threshold_avg
                    ))
                    f.write('\n')
            shutil.move('/home/tmp/amp_info.min', '/home')

    print('file resolved...')


def combine_frame(*args):
    """ 例如 87-108 被拆分为 87-106,106-108 两段或任意段，合并 """

    levels = []
    start_freq = args[0][1][4]
    pl = 0
    dl = 0
    channel_total = 0
    for i in range(len(args)):
        pl += args[i][0][4]
        dl += args[i][1][1]
        channel_total += args[i][1][8]
        stop_freq = args[i][1][5]
        levels.extend(args[i][1][-1])
    head = (args[0][0][0], args[0][0][1], args[0][0][2], args[-1][0][3], pl, args[0][0][5])
    payload = [args[0][1][0], dl, args[0][1][2], args[0][1][3], start_freq, stop_freq, args[0][1][6], args[0][1][7], channel_total, levels]

    result = (head, payload)
    return result

# a = (('eeeeeeee', 1, 812533891, '2018-9-28 19:32:11.602', 1652, 0), [12, 1619, 1, 841, 87000000.0, 106800000.0, 0, 25000.0, 793, [669, 676, 685, 683, 680, 690, 681, 681, 674, 650, 663, 658, 627, 612, 605, 602, 612, 638, 627, 676, 708, 712, 721, 726, 727, 727, 725, 721, 717, 710, 707, 701, 703, 686, 682, 657, 647, 623, 629, 623, 629, 630, 628, 620, 612, 601, 611, 583, 587, 602, 606, 605, 616, 626, 618, 611, 601, 611, 609, 599, 582, 575, 559, 572, 586, 593, 598, 611, 600, 622, 606, 595, 617, 733, 740, 748, 752, 759, 766, 761, 638, 589, 585, 584, 586, 584, 589, 599, 604, 600, 603, 609, 609, 616, 610, 599, 603, 684, 719, 716, 734, 742, 749, 748, 753, 756, 757, 756, 758, 755, 744, 743, 659, 581, 585, 576, 665, 779, 781, 758, 775, 753, 750, 754, 652, 657, 657, 662, 668, 665, 665, 668, 662, 655, 639, 622, 612, 590, 574, 578, 578, 567, 589, 569, 575, 580, 591, 587, 593, 600, 620, 719, 840, 977, 992, 991, 992, 988, 983, 971, 828, 691, 599, 600, 613, 632, 620, 594, 605, 616, 597, 611, 603, 613, 660, 659, 671, 671, 681, 683, 697, 845, 851, 836, 842, 845, 833, 790, 652, 598, 604, 600, 622, 620, 624, 627, 633, 639, 646, 653, 674, 724, 756, 759, 757, 753, 751, 751, 752, 747, 748, 731, 725, 708, 697, 683, 608, 615, 606, 603, 603, 608, 609, 604, 605, 598, 588, 588, 573, 586, 631, 621, 618, 626, 628, 630, 630, 626, 623, 597, 605, 602, 639, 640, 661, 643, 646, 655, 664, 670, 663, 662, 665, 654, 635, 648, 651, 632, 616, 578, 602, 576, 583, 582, 597, 590, 596, 616, 618, 618, 667, 692, 710, 732, 741, 744, 747, 742, 746, 748, 730, 723, 694, 675, 574, 576, 566, 572, 559, 565, 570, 572, 591, 602, 608, 629, 653, 645, 645, 645, 660, 812, 834, 860, 854, 868, 871, 864, 731, 671, 672, 674, 673, 650, 612, 618, 567, 572, 583, 582, 564, 659, 678, 704, 773, 906, 922, 932, 937, 934, 922, 894, 733, 687, 664, 620, 601, 586, 576, 580, 574, 574, 576, 594, 588, 580, 606, 644, 650, 669, 647, 646, 667, 664, 661, 672, 671, 680, 678, 676, 665, 663, 659, 651, 652, 634, 628, 580, 570, 575, 629, 615, 631, 677, 700, 704, 718, 724, 723, 724, 717, 703, 698, 695, 688, 678, 658, 634, 576, 582, 587, 594, 590, 588, 582, 580, 577, 591, 585, 582, 593, 590, 592, 618, 724, 877, 872, 869, 846, 830, 855, 858, 706, 580, 593, 588, 597, 612, 636, 648, 656, 659, 660, 671, 674, 669, 677, 674, 671, 650, 648, 652, 651, 622, 628, 583, 571, 588, 642, 643, 637, 664, 678, 682, 689, 706, 702, 703, 704, 708, 706, 711, 736, 738, 742, 740, 728, 738, 735, 723, 717, 708, 686, 651, 636, 607, 596, 594, 629, 623, 624, 640, 728, 872, 897, 879, 886, 884, 875, 822, 703, 679, 680, 663, 650, 635, 598, 621, 583, 619, 631, 620, 652, 680, 706, 713, 712, 725, 721, 734, 731, 728, 730, 743, 744, 742, 727, 712, 708, 714, 705, 702, 695, 687, 666, 617, 614, 617, 590, 599, 580, 620, 644, 668, 664, 665, 668, 673, 674, 669, 676, 666, 662, 671, 649, 636, 617, 601, 611, 602, 609, 601, 605, 606, 608, 620, 632, 623, 631, 639, 651, 656, 638, 621, 603, 602, 585, 600, 585, 598, 609, 601, 618, 629, 625, 613, 616, 615, 627, 661, 661, 674, 683, 682, 692, 697, 703, 718, 717, 721, 719, 730, 719, 720, 701, 696, 666, 590, 574, 575, 602, 580, 596, 597, 601, 609, 612, 617, 649, 650, 648, 649, 644, 646, 640, 627, 643, 622, 608, 595, 571, 581, 575, 587, 574, 577, 580, 567, 568, 568, 567, 569, 639, 715, 852, 965, 971, 976, 985, 990, 992, 994, 934, 771, 691, 613, 612, 627, 621, 605, 622, 577, 582, 599, 615, 622, 632, 634, 640, 644, 671, 662, 665, 670, 672, 678, 655, 650, 654, 652, 619, 613, 609, 587, 593, 591, 589, 576, 574, 563, 563, 572, 571, 596, 688, 770, 864, 973, 969, 965, 963, 964, 961, 950, 851, 757, 678, 624, 648, 633, 633, 625, 629, 625, 618, 622, 612, 615, 610, 610, 603, 604, 634, 626, 628, 639, 641, 649, 645, 642, 632, 620, 605, 595, 597, 619, 622, 618, 627, 633, 615, 610, 613, 683, 704, 699, 695, 694, 691, 690, 693, 694, 696, 693, 686, 682, 669, 659, 648, 645, 678, 806, 816, 823, 830, 821, 820, 803, 679, 611, 603, 604, 596, 618, 596, 590, 608, 614, 631, 632, 629, 629, 608, 595, 606, 737, 772, 785, 811, 805, 762, 732, 703, 642, 659, 661, 671, 678, 683, 672, 679, 662, 655, 664, 653, 650, 630, 631, 638, 654, 655, 668, 671]])
# b = (('eeeeeeee', 1, 812533891, '2018-9-28 19:32:11.805', 162, 0), [12, 129, 1, 841, 106825000.0, 108000000.0, 793, 25000.0, 48, [598, 588, 596, 595, 593, 609, 615, 620, 627, 619, 605, 607, 597, 576, 560, 569, 686, 749, 751, 751, 750, 752, 694, 620, 568, 568, 580, 584, 577, 595, 596, 615, 615, 619, 616, 644, 621, 641, 643, 638, 641, 629, 632, 622, 621, 621, 593, 539]])
# print(combine_frame(a, b))