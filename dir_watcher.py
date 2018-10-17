from watchdog.observers import Observer
from watchdog.events import *
import os
from tools.signal_handler import *
from tools.file_info import file_index, des_save
from tools.MyThread import MyThread


class FileEventHandler(FileSystemEventHandler):
    def __init__(self):
        FileSystemEventHandler.__init__(self)

    def on_created(self, event):
        if event.is_directory:
            pass
        else:
            try:
                file_split = os.path.split(event.src_path)
                if file_split[1].split('.')[1] == 'xml':
                    file_des = event.src_path
                    timeout = 10
                    while True:
                        timeout -= 1
                        if os.path.exists(file_split[0]+'/'+file_split[1].split('.')[0]+'.bin'):
                            file_bin = file_split[0]+'/'+file_split[1].split('.')[0]+'.bin'
                            break
                        else:
                            time.sleep(1)
                            if timeout == 0:
                                break
                    t1 = MyThread(func=file_resolve, args=(file_bin,))
                    t2 = MyThread(file_index, (file_bin, file_des, 'file_index'))
                    t3 = MyThread(file_index, (file_bin, file_des, 'task_info'))
                    t4 = MyThread(file_index, (file_bin, file_des, 'device_info'))
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
                    # time.sleep(10)
                    # print(file_bin, file_des)
                    os.remove(file_bin)
                    os.remove(file_des)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    # TODO: 移动车经纬度    file_resolve函数不能从文件名中取mfid
    # TODO: 任务数据文件规范？
    observer = Observer()
    event_handler = FileEventHandler()
    observer.schedule(event_handler, "data", True)
    observer.start()
    observer.join()
