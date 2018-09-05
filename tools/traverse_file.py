# coding=utf-8

import os


def get_all_file(dir_name, _filter=''):
    """
    遍历文件夹下所有文件,_filter:过滤文件后缀名
    """
    filters = []
    result = []
    if _filter:
        filters.append('.'+_filter)
        for main_dir, subdir, file_name_list in os.walk(dir_name):
            for file_name in file_name_list:
                fpath = os.path.join(main_dir, file_name)
                ext = os.path.splitext(fpath)[1]      # [1]文件后缀名
                if ext in filters:
                    result.append(fpath)
    else:
        for main_dir, subdir, file_name_list in os.walk(dir_name):
            for file_name in file_name_list:
                fpath = os.path.join(main_dir, file_name)
                result.append(fpath)

    return result
