#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import requests
import json
import time
import logging
import traceback
from download_class import AbuProxy, UserAgent, RequestSession
from download_public import bkdr_hash, push, random_str
from download_mysql import DbMysql


class MdHandler(object):
    def __init__(self, task, config, task_infos):
        self.task = task
        self.config = config
        # 注意感知外部task_infos的变化
        self.task_infos = task_infos
        abu_proxy = AbuProxy(self.config.proxy_user,
                             self.config.proxy_password,
                             self.config.proxy_host,
                             self.config.proxy_port)
        # 不变可共用的
        self.proxy = abu_proxy.get_proxy()
        self.rds = self.config.get_rds()
        self.ioqueuer = self.config.get_ioqueuer()
        self.mdb = DbMysql(self.config.my_host, self.config.my_port, self.config.my_user, self.config.my_password,
                           self.config.my_db)
        # 变化的
        self.agent = UserAgent()
        self.sessions = []

    def get_user_agent(self):
        return self.agent.get_agent()

    def get_account(self):
        sql = '''select account, `password` from scrapy_account where task='{0}' '''.format(self.task)
        rows = self.mdb.query(sql)
        accounts = {}
        for row in rows:
            accounts[row[0]] = row[1]
        if len(accounts) == 0:
            accounts['default'] = 'default_password'
        # self.mdb.close()
        return accounts

    def get_session(self, account, password):
        headers = {'User-Agent': self.get_user_agent()}
        sess = RequestSession(None, headers, self.proxy, self.config.time_out, account, password)
        return sess

    def get_sessions(self):
        self.sessions = []
        accounts = self.get_account()
        for account in accounts:
            sess = self.get_session(account, accounts[account])
            if sess is not None:
                self.sessions.append(sess)
                time.sleep(1)

    # 先尝试加锁
    def reget_session(self, account, password):
        sess = ''
        if account !='default':
            lock_key = 'lock_{0}_{1}_session'.format(self.task, account)
            resp = self.rds.set(lock_key, '1', ex=self.config.lock_sesseion_sec, nx=True)
            if resp is not None:
                sess = self.get_session(account, password)
        else:
            sess = self.get_session(account, password)
        return sess

    def dealing(self, task_str):
        task_info_list = self.task_infos[self.task]
        jsons = json.loads(task_str)
        url = jsons['url']
        params = jsons.get('params', {})
        if params is None:
            params = {}
        bkdr = bkdr_hash('{0}{1}'.format(url, params))
        time_path_fmt = task_info_list[1]
        if time_path_fmt != '':
            time_path = time.strftime(time_path_fmt)
            defalut_file = '{0}/{1}/{2}/{3}.html'.format(self.config.data_path, self.task, time_path, bkdr)
        else:
            defalut_file = '{0}/{1}/{2}.html'.format(self.config.data_path, self.task, bkdr)
        file_name = jsons.get('file', defalut_file)
        # 非retry任务需要去重
        if 'retry' not in jsons:
            if os.path.exists(file_name):
                if os.stat(file_name).st_size > task_info_list[5]:
                    return 1
        # 执行任务
        task = {'url': url, 'file': file_name}
        if len(params) != 0:
            task['params'] = params
        else:
            task['params'] = None
        task['except'] = task_info_list[2]
        task['notify'] = task_info_list[3]
        task['type'] = jsons.get('type', self.task)
        task['extra'] = jsons.get('extra', {})
        task['retry'] = jsons.get('retry', 0)
        status_code = self.get_content(task)
        if status_code != 200:
            if status_code == 404:
                logging.warning('url {0} 404 page not found'.format(url))
                return 404
            ret_code,ret_msg = self.ioqueuer.write(task['except'], task_str)
            if not ret_code:
                logging.warning('rpush queue[{0}][{1}] unexpect response'.format(task['except'], task))
            logging.error('download {0} failed[{1}]'.format(url, file_name))
            return -1
        return 0

    def get_content(self, tasks):
        cur_index = self.rds.incr(self.config.proxy_index, 1) % len(self.sessions)
        session = self.sessions[cur_index]
        try:
            queue = tasks['notify']
            status_code = session.download(tasks['trans_type'], tasks['url'], tasks['file'], tasks['params'], tasks['data'])
            if status_code == 200:
                logging.info('download {0} into {1}'.format(tasks['url'], tasks['file']))
                tasks['type'] = queue
            else:
                queue = tasks['except']
                sess = self.reget_session(session.account, session.password)
                if sess is not None and sess != '':
                    self.sessions[cur_index] = sess
            if queue is not None and len(queue) > 0:
                content = json.dumps(tasks)
                ret_code, ret_msg = self.ioqueuer.write(queue, content)
                if not ret_code:
                    logging.error('push queue[{0}] failed[{1}], content[{2}]'.format(queue, msg, content))
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            status_code = -1
        return status_code


class ItjuziHandler(MdHandler):
    def get_session(self, account, password):
        login_url = "https://www.itjuzi.com/user/login"
        post_data = {"identity": account, "password": password}
        user_agent = self.get_user_agent()
        headers = {'User-Agent': user_agent}
        s = requests.Session()
        try:
            s.post(login_url, data=post_data, headers=headers, proxies=self.proxy)
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            logging.error('ItjuziHandler get_session by account {0} failed'.format(account))
            return None
        sess = RequestSession(s, headers, self.proxy, self.config.time_out, account, password)
        return sess


class QichachaHandler(MdHandler):
    def get_session(self, account, password):
        login_url = 'http://www.qichacha.com/'
        user_agent = self.get_user_agent()
        headers = {'User-Agent': user_agent}
        bd_url = 'https://www.baidu.com/link?url={0}'.format(random_str(8, 24))
        headers['Referer'] = bd_url
        s = requests.Session()
        try:
            s.get(login_url, headers=headers, proxies=self.proxy)
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            logging.error('QichachaHandler get_session by account {0} failed'.format(account))
            return None
        sess = RequestSession(s, headers, self.proxy, self.config.time_out, account, password)
        return sess
