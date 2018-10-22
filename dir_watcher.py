from watchdog.observers import Observer
from watchdog.events import *
from tools.signal_handler import *
from tools.file_info import file_index, des_save, xml_parser
from tools.MyThread import MyThread
import configuration as conf


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
                    timeout = 60
                    while True:
                        timeout -= 1
                        if os.path.exists(file_split[0]+'/'+file_split[1].split('.')[0]+'.bin'):
                            file_bin = file_split[0]+'/'+file_split[1].split('.')[0]+'.bin'
                            # pass掉小于20M的监测文件
                            if os.path.getsize(file_bin)/1024**2 < 20:
                                os.remove(file_des)
                                os.remove(file_bin)
                                print('file removed...')
                                return
                            break
                        else:
                            time.sleep(1)
                            if timeout == 0:
                                break
                    xml_info = xml_parser(file_des, 'b_info')
                    mfid = xml_info[0]
                    start_freq = xml_info[1]/1000000
                    stop_freq = xml_info[2]/1000000

                    t1 = MyThread(func=file_resolve, args=(file_bin, mfid, start_freq, stop_freq))
                    t2 = MyThread(file_index, (file_bin, file_des, 'file_index'))
                    t3 = MyThread(file_index, (file_bin, file_des, 'task_info'))
                    t4 = MyThread(file_index, (file_bin, file_des, 'device_info'))
                    # t5 = MyThread(des_save, (file_des,))

                    t1.start()
                    t2.start()
                    t3.start()
                    t4.start()
                    # t5.start()

                    res = [t1, t2, t3, t4]
                    for t in res:
                        t.join()
                        print('done')
                        # print(t.get_result())
                    os.remove(file_bin)
                    os.remove(file_des)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    # TODO: 移动车经纬度    file_resolve函数不能从文件名中取mfid
    observer = Observer()
    event_handler = FileEventHandler()
    observer.schedule(event_handler, conf.ftp_tmp_path, True)
    observer.start()
    observer.join()
