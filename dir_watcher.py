from watchdog.observers import Observer
from watchdog.events import *
from tools.signal_handler import *
from tools.file_info import file_index, des_save, xml_parser, get_file_info
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
                    timeout = 60
                    while True:
                        timeout -= 1
                        if os.path.exists(file_split[0]+'/'+file_split[1].split('.')[0]+'.bin'):
                            file_bin = file_split[0]+'/'+file_split[1].split('.')[0]+'.bin'
                            # 判断文件是否传输完毕
                            while True:
                                oldl = os.path.getsize(file_bin)
                                time.sleep(1)
                                currentl = os.path.getsize(file_bin)
                                if currentl == oldl:
                                    break
                            # 移除小于20M的监测文件
                            if os.path.getsize(file_bin) < 20971520:
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
                    file_info = get_file_info(file_bin)
                    file_size_min = file_info[-1]
                    data_type = file_info[2]

                    file_resolve(file_bin, mfid, start_freq, stop_freq, file_size_min, data_type)
                    # t1 = MyThread(func=file_resolve, args=(file_bin, mfid, start_freq, stop_freq, file_size_min, data_type))
                    t2 = MyThread(file_index, (file_bin, file_des, 'file_index'))
                    t3 = MyThread(file_index, (file_bin, file_des, 'task_info'))
                    t4 = MyThread(file_index, (file_bin, file_des, 'device_info'))
                    t5 = MyThread(des_save, (file_des,))

                    # t1.start()
                    t2.start()
                    t3.start()
                    t4.start()
                    t5.start()

                    # res = [t1, t2, t3, t4, t5]
                    res = [t2, t3, t4, t5]
                    for t in res:
                        t.join()
                        print('done')
                        # print(t.get_result())
                    os.remove(file_bin)
                    os.remove(file_des)

                    # 记录正常处理后文件
                    with open('/home/fileresolve_log/correct.log', 'a') as f:
                        f.write(os.path.basename(file_des)+'--'+os.path.basename(file_bin)+'\n')

            except Exception as e:
                with open('/home/fileresolve_log/error.log', 'a') as f:
                    f.write(os.path.basename(file_bin)+'--'+str(e)+'\n')
                print('error at: ' + file_bin)


if __name__ == '__main__':

    observer = Observer()
    event_handler = FileEventHandler()
    observer.schedule(event_handler, '/home/ftp', True)
    observer.start()
    observer.join()
