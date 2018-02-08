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

import datetime

from gain_func import ElementExtractor
from gain_public import incr
from gain_config import GaInConfig

"""
    task['type']
    task['url']
    task['params']
    task['file']
    task['except']
    task['notify']
    task['retry']
"""


class GaInThread(object):
    def __init__(self, gain_config):
        self.gain_config = gain_config
        self.element_extractor = ElementExtractor()
        self.threads = []
        self.tasks = {}
        self.rules = {}
        self.hour_index = datetime.datetime.now().hour
        self.fb = None

    # 处理线程 读取任务
    def deal(self, task_queue):
        rds_cli = self.gain_config.get_rds()
        task = None
        status = -1
        while True:
            try:
                # 检查是否需要退出
                response = rds_cli.get(self.gain_config.stop_key)
                if response is not None and response[0] == '1':
                    logging.info('get stop_download event')
                    break
                # 获取任务
                task_str = rds_cli.lpop(task_queue)
                if task_str is None:
                    time.sleep(1)
                    continue
                # json解析
                task = json.loads(task_str)
                if 'type' not in task:
                    task['type'] = task_queue
                incr(rds_cli, self.gain_config.stat_hash, task_queue, 'request')
                status, element_dic = self.element_extractor.get_html_element(rds_cli, self.rules, task,
                                                                              self.tasks[task_queue])

                if status != 0:
                    incr(rds_cli, self.gain_config.stat_hash, task_queue, 'fail')
                    logging.error('deal task {0} failed[{1}]'.format(task, status))
                    # 异常队列
                    except_queue = self.tasks[task_queue][2]
                    resp = rds_cli.rpush(except_queue, task_str)
                    if resp is None:
                        logging.warning('rpush queue[{0}][{1}] unexpect response'.format(except_queue, task))
                else:
                    incr(rds_cli, self.gain_config.stat_hash, task_queue, 'success')
                    # 输出适配
                    if self.gain_config.out_flag.find('file') > -1:
                        # 写文件前获取当前时间，并判断是否需要创建新的时间片文件
                        curtime = datetime.datetime.now()
                        if self.fb:
                            if self.hour_index != curtime.hour:
                                self.hour_index = curtime.hour
                                self.fb.close()
                                file_name = '{0}.{1}'.format(task_queue, curtime.strftime("%Y-%m-%d-%H"))
                                self.fb = open(self.gain_config.out_path + file_name, 'a', encoding='utf-8')
                        else:
                            file_name = '{0}.{1}'.format(task_queue, curtime.strftime("%Y-%m-%d-%H"))
                            self.fb = open(self.gain_config.out_path + file_name, 'a', encoding='utf-8')
                        self.fb.write(json.dumps(element_dic) + '\n')

                    if self.gain_config.out_flag.find('hash') > -1:
                        if self.element_extractor.store_element != '':
                            store_hash = self.tasks[task_queue][4]
                            logging.info(
                                'try store_record {0} {1} {2}'.format(store_hash, self.element_extractor.store_element,
                                                                      json.dumps(element_dic)))
                            try:
                                rds_cli.hset(store_hash, self.element_extractor.store_element, json.dumps(element_dic))
                            except Exception as e:
                                logging.error(e)
                                logging.error(traceback.format_exc())
                            next_queue = self.tasks[task_queue][3]
                            if next_queue != '':
                                logging.info('try notify to queue {0} value {1}'.format(next_queue,
                                                                                        self.element_extractor.store_element))
                                try:
                                    rds_cli.rpush(next_queue, self.element_extractor.store_element)
                                except Exception as e:
                                    logging.error(e)
                                    logging.error(traceback.format_exc())
            except Exception as e:
                incr(rds_cli, self.gain_config.stat_hash, task_queue, 'fail')
                logging.error(e)
                logging.error(traceback.format_exc())
                logging.error('deal task {0} exception[{1}]'.format(task, status))
                continue

    # 主线程, 定期扫描mysql task表, 对于新增的任务创建处理线程
    def run(self):
        rds_cli = self.gain_config.get_rds()
        while True:
            response = rds_cli.get(self.gain_config.stop_key)
            if response is not None:
                # 退出
                if response[0] == '1':
                    logging.info('monitor get stop_download event, thread quit.')
                    time.sleep(3)
                    break
                # 同步rules  2:task_1,task_2
                if response[0] == '2':
                    logging.info('monitor get update rules.')
                    infos = response.split(':')
                    if len(infos) > 1:
                        tasks = infos[1].split(',')
                        for task in tasks:
                            self.rules[task] = self.element_extractor.get_rules(self.gain_config.new_mdb(), task)
                            logging.info('update task {0} rules'.format(task))
                if response[0] == '3':
                    logging.info('monitor get update tasks.')
                    self.gain_config.new_mdb()
            new_tasks = self.element_extractor.get_tasks(self.gain_config.get_mdb(), self.gain_config.run_seqs)
            # 检查到新任务 自己启动和处理
            for new_task in new_tasks:
                if new_task not in self.tasks:
                    self.rules[new_task] = self.element_extractor.get_rules(self.gain_config.get_mdb(), new_task)
                    self.tasks[new_task] = new_tasks[new_task]
                    logging.info('get new task {0} {1}'.format(new_task, self.tasks[new_task]))
                    new_thread_num = self.tasks[new_task][0]
                    for idx in range(new_thread_num):
                        new_thread = threading.Thread(target=self.deal,
                                                      args=(new_task,))
                        new_thread.setDaemon(True)
                        new_thread.start()
                        self.threads.append(new_thread)
                else:
                    if not operator.eq(new_tasks[new_task], self.tasks[new_task]):
                        logging.info('task {0} info changed {1}'.format(new_task, self.tasks[new_task]))
                        self.tasks[new_task] = new_tasks[new_task]

            self.gain_config.commit()
            time.sleep(10)

    def join(self):
        for t in self.threads:
            t.join()
            logging.info('{0} exit now'.format(t.getName()))


class TestAbuThread(unittest.TestCase):
    @staticmethod
    def init():
        log_conf = './log.conf'
        log_file = './test_abu_thread.log'
        conf_file = './gain_config.conf'
        try:
            os.remove(log_file)
        except Exception as e:
            print(e)
        with open(log_conf, 'w') as fp:
            fp.write('[loggers]\n')
            fp.write('keys = root\n')
            fp.write('[logger_root]\n')
            fp.write('handlers = root\n')
            fp.write('qualname = root\n')
            fp.write('level = INFO\n')
            fp.write('[handlers]\n')
            fp.write('keys = root\n')
            fp.write('[handler_root]\n')
            fp.write('class = logging.handlers.TimedRotatingFileHandler\n')
            fp.write('formatter = root\n')
            fp.write('args = ("{0}", "H", 1, 30)\n'.format(log_file))
            fp.write('[formatters]\n')
            fp.write('keys = root\n')
            fp.write('[formatter_root]\n')
            fp.write('format = %(asctime)s [%(levelname)s] [%(filename)s:%(lineno)d] %(message)s\n')
            fp.write('datefmt = %Y-%m-%d %H:%M:%S\n')
            fp.write('\n')
        logging.config.fileConfig(log_conf)

        with open(conf_file, 'w') as fp:
            fp.write('[data]\n')
            fp.write('stop_key = event_itjuzi_analyze\n')
            fp.write('[redis]\n')
            fp.write('host = 127.0.0.1\n')
            fp.write('port = 21601\n')
            fp.write('password = Jim123\n')
            fp.write('[mysql]\n')
            fp.write('host = 127.0.0.1\n')
            fp.write('port = 3306\n')
            fp.write('user = jim\n')
            fp.write('password = 111111\n')
            fp.write('db = rta\n')
        return conf_file

    @staticmethod
    def clear():
        log_conf = './log.conf'
        # log_file = './test_abu_thread.log'
        conf_file = './gain_config.conf'
        os.remove(conf_file)
        os.remove(log_conf)
        # os.remove(log_file)

    def test_run(self):
        conf_file = self.init()
        gain_config = GaInConfig(conf_file)
        rds = gain_config.get_rds()
        print(rds.ping())
        mdb = gain_config.get_mdb()
        print(mdb)

        abu_thread = GaInThread(gain_config)
        print('\n\ntry to set event_download after several seconds!!!')
        rds = gain_config.get_rds()
        rds.delete('event_itjuzi_analyze')
        abu_thread.run()
        # 需要人工设置停止 redis-cli -p 21601 -a Jim123 set event_download 1
        abu_thread.join()
        self.clear()
