#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from download_handler import *


# 读取mysql任务表有效的任务信息
def get_tasks(mdb_cli, task_seqs):
    tasks = {}
    sql = '''select task, thread_num, time_path, exception_queue, next_task,
            handler, valid_size from scrapy_task where `status`=1 and seq in ({0})'''.format(task_seqs)
    rows = mdb_cli.query(sql)
    for row in rows:
        task, thread_num, time_path, exception_queue, next_task, handler, valid_size = row[:]
        tasks[task] = [thread_num, time_path, exception_queue, next_task, handler, valid_size]
    return tasks


def get_handler(config, task, task_infos):
    if task not in task_infos:
        logging.error('task {0} not found in task_infos'.format(task))
        return None
    infos = task_infos[task]
    handler_class = eval(infos[4])
    handler = handler_class(task, config, task_infos)
    handler.get_sessions()
    return handler
