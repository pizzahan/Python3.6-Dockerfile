#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import threading
import operator
import traceback
import unittest
import logging
import logging.config

from dbpusher_func import get_tasks, get_funcs, deal_task
from dbpusher_config import DbConfig


class DbThread(object):
    def __init__(self, config):
        self.config = config
        self.threads = []
        self.tasks = {}
        self.funcs = {}

    # 处理线程 读取任务
    def deal(self, task_queue):
        rds_cli = self.config.get_rds()
        mdb_cli = self.config.get_mdb()
        task = None
        deal_num = 0
        status = -1
        while True:
            try:
                # 检查是否需要退出
                response = rds_cli.get(self.config.stop_key)
                if response is not None:
                    if response[0] == '1':
                        logging.info('get stop_dbloader event')
                        if mdb_cli is not None and deal_num != 0:
                            mdb_cli.commit()
                        break
                # 获取任务
                task_str = rds_cli.lpop(task_queue)
                if task_str is None:
                    if deal_num != 0:
                        mdb_cli.commit()
                        deal_num = 0
                    time.sleep(1)
                    continue
                logging.info('get task {0}'.format(task_str))
                status = deal_task(mdb_cli, rds_cli, task_str, self.tasks[task_queue], self.funcs[task_queue])
                if status > 0:
                    # 重新连接
                    mdb_cli = self.config.get_mdb()
                    status = deal_task(mdb_cli, rds_cli, task_str, self.tasks[task_queue], self.funcs[task_queue])
                if status == 0:
                    deal_num += 1
                    if deal_num >= 100:
                        mdb_cli.commit()
                        deal_num = 0
                else:
                    logging.error('deal task {0} failed'.format(task_str))
            except Exception as e:
                if deal_num != 0:
                    mdb_cli.commit()
                    deal_num = 0
                mdb_cli = None
                logging.error(e)
                logging.error(traceback.format_exc())
                logging.error('dbloader deal task {0} failed[{1}]'.format(task, status))
                continue

    # 主线程, 定期扫描mysql task表, 对于新增的任务创建处理线程
    def run(self):
        rds_cli = self.config.get_rds()
        mdb_conn = self.config.get_mdb()
        while True:
            response = rds_cli.get(self.config.stop_key)
            if response is not None:
                # 退出
                if response[0] == '1':
                    logging.info('monitor get stop event, thread quit.')
                    time.sleep(3)
                    break
                # 同步规则
                if response[0] == '2':
                    logging.info('monitor get sync event, thread quit.')
                    time.sleep(3)

                # 重新连接数据库
                if response[0] == '3':
                    logging.info('monitor get reconnect signal.')
                    mdb_conn = self.config.get_mdb()
            new_tasks = get_tasks(mdb_conn, self.config.run_seqs)
            # 检查到新任务 自己启动和处理
            for new_task in new_tasks:
                if new_task not in self.tasks:
                    self.tasks[new_task] = new_tasks[new_task]
                    logging.info('get new task {0} {1}'.format(new_task, self.tasks[new_task]))
                    self.funcs.setdefault(new_task, {})
                    self.funcs[new_task] = get_funcs(mdb_conn, new_tasks[new_task].keys())
                    logging.info('get new task functions {0} {1}'.format(new_task, self.funcs[new_task]))
                    new_thread_num = 1
                    for idx in range(new_thread_num):
                        new_thread = threading.Thread(target=self.deal, args=(new_task,))
                        new_thread.setDaemon(True)
                        new_thread.start()
                        self.threads.append(new_thread)
                else:
                    if not operator.eq(new_tasks[new_task], self.tasks[new_task]):
                        logging.info('''update task {0} info, before {1},
                                        after {2}
                                    '''.format(new_task, self.tasks[new_task], new_tasks[new_task]))
                        self.tasks[new_task] = new_tasks[new_task]
            # 没有意义, 但是不commit看不到数据库中的变化
            mdb_conn.commit()
            time.sleep(10)

    def join(self):
        for t in self.threads:
            t.join()
            logging.info('{0} exit now'.format(t.getName()))


class TestDbThread(unittest.TestCase):
    def test_run(self):
        conf_file = './conf/dbpusher.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)
        gain_config = DbConfig(conf_file)

        abu_thread = DbThread(gain_config)
        print('\n\ntry to set event_gain_dbpusher after several seconds!!!')
        rds = gain_config.get_rds()
        rds.delete('event_gain_dbpusher')
        abu_thread.run()
        # 需要人工设置停止 redis-cli -p 21601 -a Mindata123 set event_gain_dbpusher 1
        abu_thread.join()
