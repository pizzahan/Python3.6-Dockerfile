#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import re
import time
import json
import traceback
import logging


# 判断队列长度,如果超过监控长度,暂停写入
def push(rds, queue, msg):
    try:
        if queue:
            while 1:
                length = rds.llen(queue)
                if length < 1000000:
                    break
                time.sleep(1)
            length = rds.rpush(queue, msg)
            return length
        else:
            logging.error('queue {0} is empty'.format(queue))
            return -1
    except Exception as e:
        logging.error('rds {0} push to queue {1} msg {2} failed[{3}]'.format(rds, queue, msg, e))
        return -1


# 记录redis指标
def incr(rds, stor_hash, field_queue, field_type, number=1):
    cur_time = time.strftime("%Y%m%d%H", time.localtime())
    field = '{0}|{1}|{2}|{3}'.format(cur_time[:8], cur_time[8:10], field_queue, field_type)
    try:
        rds.hincrby(stor_hash, field, number)
    except Exception as e:
        logging.error(e)
        logging.error('incr {0} {1} {2} failed'.format(stor_hash, field, number))


# 读取mysql任务表有效的任务信息
def get_tasks(connection, task_seqs):
    tasks = {}
    with connection.cursor() as cursor:
        try:
            sql = '''select task, exception_queue, retry_num, pre_task from scrapy_task
                    where status=1 and seq in ({0})'''.format(task_seqs)
            cursor.execute(sql)
            rows = cursor.fetchall()
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            return tasks

        for row in rows:
            task, except_queue, retry_num, pre_task = row[0], row[1], row[2], row[3]
            if except_queue == '':
                logging.warning('task {0} has no exception'.format(task))
                continue
            tasks[task] = [except_queue, retry_num, pre_task]
    return tasks


def redeal_except_task(rds, task, task_infos):
    status = -1
    if 'type' not in task:
        logging.error('unexpect exception {0}'.format(task))
        return status
    if task['type'] not in task_infos:
        logging.warning('task type no infos {0} '.format(task['type']))
        return -1
    task_info_list = task_infos[task['type']]
    max_retry_num = task_info_list[1]
    pre_task = task_info_list[2]
    task['retry'] = task.get('retry', 0) + 1
    if task['retry'] > max_retry_num:
        logging.error('redeal task {0} over max retry times {1}'.format(task, max_retry_num))
        return status
    queue = task['type']
    analyze_pattern = re.compile(r'analyze')
    match = analyze_pattern.match(task['type'])
    if match:
        queue = pre_task
        task['type'] = pre_task
    length = push(rds, queue, json.dumps(task))
    if length > 0:
        logging.info('redeal task {0} rpush to {1}'.format(task, queue))
        status = 0
    return status
