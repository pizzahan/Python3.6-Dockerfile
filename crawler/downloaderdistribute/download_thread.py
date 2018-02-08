#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import threading
import operator
import traceback
import unittest
import logging
import logging.config

from download_func import get_tasks, get_handler
from download_public import incr
from download_config import MdConfig
from download_mysql import DbMysql

"""
    task['type']
    task['url']
    task['params']
    task['file']
    task['except']
    task['notify']
    task['extra']
    task['retry']
"""


class MdThread(object):
    def __init__(self, config):
        self.config = config
        self.threads = []
        self.tasks = {}
        self.handlers = {}

    # 处理线程 读取任务 获取url并进行下载, 下载后通知解析处理
    def deal(self, task_queue):
        rds_cli = self.config.get_rds()
        handler = self.handlers[task_queue]
        while True:
            try:
                # 读取现成控制参数
                response = rds_cli.get(self.config.stop_key)
                if response is not None:
                    # 退出
                    if response[0] == '1':
                        logging.info('{0} get stop_download event'.format(task_queue))
                        break
                # 获取任务
                task_str = rds_cli.lpop(task_queue)
                if task_str is None:
                    time.sleep(1)
                    continue
                incr(rds_cli, self.config.stat_hash, task_queue, 'request')
                status_code = handler.dealing(task_str)
                if status_code != 0:
                    if status_code == 1:
                        incr(rds_cli, self.config.stat_hash, task_queue, 'exists')
                    else:
                        incr(rds_cli, self.config.stat_hash, task_queue, 'fail')
                else:
                    incr(rds_cli, self.config.stat_hash, task_queue, 'success')
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                incr(rds_cli, self.config.stat_hash, task_queue, 'fail')
                continue

    # 主线程, 定期扫描mysql task表, 对于新增的任务创建处理线程
    def run(self):
        rds_cli = self.config.get_rds()
        # mdb_cli = self.config.get_mdb()
        mdb_cli = DbMysql(self.config.my_host, self.config.my_port, self.config.my_user, self.config.my_password, self.config.my_db)
        while True:
            response = rds_cli.get(self.config.stop_key)
            if response is not None:
                # 退出
                if response[0] == '1':
                    logging.info('monitor get stop_download event, thread quit.')
                    time.sleep(3)
                    mdb_cli.close()
                    break
            new_tasks = get_tasks(mdb_cli, self.config.run_seqs)
            # 检查到新任务 自己启动和处理
            for new_task in new_tasks:
                if new_task not in self.tasks:
                    self.tasks[new_task] = new_tasks[new_task]
                    logging.info('get new task {0} {1}'.format(new_task, self.tasks[new_task]))
                    handler = get_handler(self.config, new_task, self.tasks)
                    if handler is None:
                        logging.error('get_handler {0} failed'.format(new_task))
                        continue
                    else:
                        self.handlers[new_task] = handler
                    new_thread_num = self.tasks[new_task][0]
                    for idx in range(new_thread_num):
                        new_thread = threading.Thread(target=self.deal, args=(new_task, ))
                        new_thread.setDaemon(True)
                        new_thread.start()
                        self.threads.append(new_thread)
                else:
                    if not operator.eq(new_tasks[new_task], self.tasks[new_task]):
                        logging.info('task {0} info changed {1}'.format(new_task, self.tasks[new_task]))
                        self.tasks[new_task] = new_tasks[new_task]
            mdb_cli.commit()
            time.sleep(10)

    def join(self):
        for t in self.threads:
            t.join()
            logging.info('{0} exit now'.format(t.getName()))


class TestMdThread(unittest.TestCase):
    def test_run(self):
        conf_file = './conf/downloader.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)

        abu_config = MdConfig(conf_file)
        rds = abu_config.get_rds()
        print(rds.ping())

        abu_thread = MdThread(abu_config)
        print('\n\ntry to set event_gain_downloader after several seconds!!!')
        abu_thread.run()
        # 需要人工设置停止 redis-cli -p 21601 -a Mindata123 set event_gain_downloader 1
        abu_thread.join()
