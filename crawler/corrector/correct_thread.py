#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import threading
import json
import operator
import traceback
import unittest
import logging
import logging.config

from correct_func import get_tasks, redeal_except_task, incr
from correct_config import CorrectConfig

"""
    task['type']
    task['url']
    task['params']
    task['file']
    task['except']
    task['notify']
    task['retry']
"""


class CorrectThread(object):
    def __init__(self, correct_config):
        self.correct_config = correct_config
        self.threads = []
        self.tasks = {}

    # 处理线程 读取任务
    def deal(self, task_queue):
        rds_cli = self.correct_config.get_rds()
        task = None
        status = -1
        while True:
            try:
                # 检查是否需要退出
                response = rds_cli.get(self.correct_config.stop_key)
                if response is not None and response[0] == '1':
                    logging.info('get stop_correct event')
                    break
                # 获取任务
                task_info_list = self.tasks[task_queue]
                except_queue = task_info_list[0]
                task_str = rds_cli.lpop(except_queue)
                if task_str is None:
                    time.sleep(1)
                    continue
                # json解析
                task = json.loads(task_str)
                incr(rds_cli, self.correct_config.stat_hash, task_queue, 'request')
                status = redeal_except_task(rds_cli, task, self.tasks)
                if status != 0:
                    incr(rds_cli, self.correct_config.stat_hash, task_queue, 'fail')
                else:
                    incr(rds_cli, self.correct_config.stat_hash, task_queue, 'success')
            except Exception as e:
                incr(rds_cli, self.correct_config.stat_hash, task_queue, 'fail')
                logging.error(e)
                logging.error(traceback.format_exc())
                logging.error('redeal task {0} failed[{1}]'.format(task, status))
                continue

    # 主线程, 定期扫描mysql task表, 对于新增的任务创建处理线程
    def run(self):
        rds_cli = self.correct_config.get_rds()
        while True:
            response = rds_cli.get(self.correct_config.stop_key)
            if response is not None:
                # 退出
                if response[0] == '1':
                    logging.info('monitor get stop_correct event, thread quit.')
                    time.sleep(3)
                    break
                # 重新连接数据库
                if response[0] == '3':
                    logging.info('monitor get reconnect signal.')
                    self.correct_config.new_mdb()
            new_tasks = get_tasks(self.correct_config.get_mdb(), self.correct_config.run_seqs)
            # 检查到新任务 自己启动和处理
            for new_task in new_tasks:
                if new_task not in self.tasks:
                    self.tasks[new_task] = new_tasks[new_task]
                    logging.info('get new task {0} {1}'.format(new_task, self.tasks[new_task]))
                    new_thread_num = 1
                    for idx in range(new_thread_num):
                        new_thread = threading.Thread(target=self.deal, args=(new_task, ))
                        new_thread.setDaemon(True)
                        new_thread.start()
                        self.threads.append(new_thread)
                else:
                    if not operator.eq(new_tasks[new_task], self.tasks[new_task]):
                        logging.info(
                            'update task {0} info, before {1}, after {2}'.format(new_task, self.tasks[new_task],
                                                                                 new_tasks[new_task]))
                        self.tasks[new_task] = new_tasks[new_task]
            self.correct_config.commit()
            time.sleep(10)

    def join(self):
        for t in self.threads:
            t.join()
            logging.info('{0} exit now'.format(t.getName()))


class TestCorrectThread(unittest.TestCase):
    def test_run(self):
        conf_file = './conf/corrector.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)
        correct_config = CorrectConfig(conf_file)
        rds = correct_config.get_rds()
        print(rds.ping())
        mdb = correct_config.get_mdb()
        print(mdb)

        abu_thread = CorrectThread(correct_config)
        print('\n\ntry to set event_correct after several seconds!!!')
        rds = correct_config.get_rds()
        rds.delete('event_itjuzi_correct')
        abu_thread.run()
        # 需要人工设置停止 redis-cli -p 21601 -a Jim123 set event_download 1
        abu_thread.join()
        self.clear()
