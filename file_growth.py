from watchdog.observers import Observer
from watchdog.events import *
import time


class FileEventHandler(FileSystemEventHandler):
    def __init__(self):
        FileSystemEventHandler.__init__(self)

    def on_created(self, event):
        if event.is_directory:
            pass
        else:
            file = event.src_path
            file_basename = os.path.basename(file)
            if file_basename.split('.')[-1] == 'bin':
                file_size = str(round(os.path.getsize(file)/1024/1024, 2))
                current_time = time.strftime('%Y-%m-%d %H:%M', time.localtime(time.time()))

                with open('D:\\log', 'a') as f:
                    f.write(file_basename+'|'+file_size+'|'+current_time)
                    f.write('\n')

                print(file_basename)


if __name__ == '__main__':
    try:
        observer = Observer()
        event_handler = FileEventHandler()
        observer.schedule(event_handler, '.', True)
        observer.start()
        observer.join()
    except Exception as e:
        print(e)
