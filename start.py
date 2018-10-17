from tools.signal_handler import *
from tools.file_info import file_index, des_save
from tools.MyThread import MyThread


if __name__ == '__main__':
    # TODO: 移动车经纬度文件
    file_data = '7adb8062-4ace-4269-ab11-6f019a9fe0db_20180928153703.bin'
    # file_des = '7adb8062-4ace-4269-ab11-6f019a9fe0db_20180928153703.xml'
    file_des = '52010001119001-B_PScan(VHF)-838a7074-ff73-49c3-a65d-86dd0ec967dd-20180801152800.0115.des'

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

