import numpy as np
import pandas as pd


def binary_search(lis, key):
    """
    如果查找到列表中相同值，则返回 True 并返回index
    列表中无相同值，则返回 False 并返回列表中大小最接近值的index
    """
    length = len(lis)
    low = 0
    heigth = length - 1

    while low <= heigth:
        mid = int((low + heigth) / 2)

        if key > lis[mid]:
            low = mid + 1
        elif key < lis[mid]:
            heigth = mid - 1
        else:
            return True, mid
    return False, mid


def signal_match(sobj_list, match_list):
    """
    函数接收参数为信号对象列表 [sObj1, sObj2...]
    一个信号对象 sObj = [elem1, elem2...]
    如果参数中只有一个信号对象，传入格式 [[elem1, elem2...]]

    match_list 匹配列表，其中只包含信号中freq字段

    函数为生成器
    """
    for sobj in sobj_list:
        flag, index = binary_search(match_list, sobj[3])
        sobj[3] = match_list[index]
        yield sobj


if __name__ == '__main__':

    csvfile = pd.read_csv('part-00000-6c72060f-6b00-4daf-acec-0f5a51567fc2-c000.csv')
    freq_array = np.unique(np.array(csvfile['freq']))

    signal_list = [
        [1,'2018-12-22T12:29:12.000+08:00','2018-12-22T12:59:08.000+08:00',400050.0,9.3,-2.4,75.0,124,52010000110002,0,0.5488,'6d4f3e58-f72f-11e8-828c-7cd30a552050'],
        [1,'2018-12-22T13:00:14.000+08:00','2018-12-22T13:55:09.000+08:00',200050.0,9.8,-10.2,650.0,1512,52010000110002,0,0.4527,'6d4f3e58-f72f-11e8-828c-7cd30a552050'],
        [1,'2018-12-22T12:29:12.000+08:00','2018-12-22T12:59:08.000+08:00',604325.0,2.6,-11.2,400.0,527,52010000110002,0,0.2805,'6d4f3e58-f72f-11e8-828c-7cd30a552050'],
        [1,'2018-12-22T13:00:14.000+08:00','2018-12-22T13:55:09.000+08:00',406625.0,7.1,-10.7,1525.0,3472,52010000110002,0,1.0,'6d4f3e58-f72f-11e8-828c-7cd30a552050'],
        [1,'2018-12-22T12:29:12.000+08:00','2018-12-22T12:59:08.000+08:00',801825.0,5.7,-10.4,1450.0,1798,52010000110002,0,0.6463,'6d4f3e58-f72f-11e8-828c-7cd30a552050'],
        [1,'2018-12-22T12:29:12.000+08:00','2018-12-22T12:59:08.000+08:00',902750.0,9.2,-10.1,1075.0,1364,52010000110002,0,1.0,'6d4f3e58-f72f-11e8-828c-7cd30a552050']
    ]

    for sobj in signal_match(signal_list, freq_array):
        print(sobj)
