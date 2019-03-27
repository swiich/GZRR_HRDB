# coding=utf-8
from __future__ import print_function, division
import os


def watcher(dir, size):
    usage_size = get_dir_usage_size(dir)
    if usage_size >= size:
        try:
            os.popen('rm -rf {0}'.format(dir))
        except Exception as e:
            print(e)


def get_dir_usage_size(dir_path):
    """unit of size: Gb"""
    response = os.popen('du -s {0}'.format(dir_path))
    # get size from popen object
    str_size = response.read().split()[0]
    usage_size = round(int(str_size) / 1024 ** 2, 1)

    return usage_size


watcher('/home/x/Pictures',0)


