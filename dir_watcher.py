from watchdog.observers import Observer
from watchdog.events import *
import os
from tools.file_info import xml_parser


class FileEventHandler(FileSystemEventHandler):
    def __init__(self):
        FileSystemEventHandler.__init__(self)

    def on_created(self, event):
        if event.is_directory:
            pass
        else:
            try:
                file = event.src_path
                print(xml_parser(file, 'file_index'))
            except Exception as e:
                print(e)
            finally:
                os.remove(file)


if __name__ == '__main__':
    observer = Observer()
    event_handler = FileEventHandler()
    observer.schedule(event_handler, "data", True)
    observer.start()
    observer.join()
