#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import redis
import logging
import logging.config
# from redis.sentinel import Sentinel

from download_mysql import DbMysql
from ai_output import *


class MdConfig(object):
    def __init__(self):
        env_dic = os.environ

        logging.error('env:{0}'.format(env_dic))
        # redis config
        '''
        sentinel_hosts=env_dic.get('sentinel_hosts').split(',')
        sentinel_ports=env_dic.get('sentinel_ports').split(',')
        sentinel_list = []
        for i in range(len(sentinel_hosts)):
            sentinel_list.append( (sentinel_hosts[i],sentinel_ports[i]) )
        sentinel = Sentinel(sentinel_list,
            socket_timeout=0.1)
        self.rds = sentinel.master_for(env_dic.get('redis_master'), socket_timeout=0.1, password=env_dic.get('redis_password'))
        '''
        self.rds = None
        self.queuer = None
        self.storer = None
        self.file_writer = None
        # inputer config
        # redis
        self.redis_host = env_dic.get('redis_host')
        self.redis_port = env_dic.get('redis_port')
        self.redis_db = env_dic.get('redis_db')
        self.redis_password = env_dic.get('redis_password')
        self.redis_password = env_dic.get('redis_password')
        self.redis_password = env_dic.get('redis_password')
        # kafka
        self.kafka_host = env_dic.get('kafka_host')
        self.kafka_retries = env_dic.get('kafka_retries')
        self.kafka_username = env_dic.get('kafka_username')
        self.kafka_password = env_dic.get('kafka_password')
        self.kafka_group = env_dic.get('kafka_group')
        # file
        self.filename = env_dic.get('filename')
        self.file_mode = env_dic.get('file_mode')
        self.file_encoding = env_dic.get('file_encoding')

        # stop key
        self.run_seqs = env_dic.get('download_run_seqs')
        self.stop_key = env_dic.get('download_stop_key')
        self.data_path = env_dic.get('download_data_path')
        self.stat_hash = env_dic.get('download_stat_hash')
        self.proxy_key = env_dic.get('download_proxy_key')
        self.proxy_index = env_dic.get('download_proxy_index')
        self.lock_sesseion_sec = env_dic.get('download_lock_sesseion_sec')
        self.time_out = int(env_dic.get('download_time_out', '3'))

        # proxy config
        self.rds = self.get_rds()
        self.proxy_host = self.rds.hget(self.proxy_key, 'host')
        self.proxy_port = self.rds.hget(self.proxy_key, 'port')
        self.proxy_user = self.rds.hget(self.proxy_key, 'user')
        self.proxy_password = self.rds.hget(self.proxy_key, 'password')

        # mysql config
        self.my_host = env_dic.get('mysql_host')
        self.my_port = int(env_dic.get('mysql_port'))
        self.my_user = env_dic.get('mysql_user')
        self.my_password = env_dic.get('mysql_password')
        self.my_db = env_dic.get('mysql_db')
        self.mdb = DbMysql(self.my_host, self.my_port, self.my_user, self.my_password, self.my_db)
        logging.info('mysql host[{0}] port[{1}]'.format(self.my_host, self.my_port))

        # 输入输出类型
        self.input_flag = env_dic.get('input_flag')
        self.output_flag = env_dic.get('output_flag')

    # redis线程安全, 共用一个连接即可, mysql不行
    def get_rds(self):
        self.rds = redis.Redis(host=self.redis_host,
                               port=self.redis_port,
                               db=0,
                               password=self.redis_password,
                               decode_responses=True
                               )
        return self.rds

    def get_mdb(self):
        return self.mdb

    def get_queuer(self, input_flag):
        if input_flag.find('redis') > -1:
            self.queuer = IOQueue('redis', self.redis_host, self.redis_port, self.redis_db,
                                  password=self.redis_password, retries=self.kafka_retries,
                                  username=self.kafka_username,
                                  group=self.kafka_group)
            ret_code, ret_msg = self.queuer.open()
            if not ret_code:
                print(ret_msg)
        elif input_flag.find('kafka') > -1:
            self.queuer = IOQueue('kafka', self.kafka_host, self.redis_port, self.redis_db,
                                  password=self.redis_password, retries=self.kafka_retries,
                                  username=self.kafka_username,
                                  group=self.kafka_group)
            ret_code, ret_msg = self.queuer.open()
            if not ret_code:
                print(ret_msg)
        return self.queuer

    def get_storer(self, output_flag):
        if output_flag.find('redis') > -1:
            self.storer = IOQueue('redis', self.redis_host, self.redis_port, self.redis_db,
                                  password=self.redis_password, retries=self.kafka_retries,
                                  username=self.kafka_username,
                                  group=self.kafka_group)
            ret_code, ret_msg = self.storer.open()
            if not ret_code:
                print(ret_msg)
        elif output_flag.find('kafka') > -1:
            self.storer = IOQueue('kafka', self.kafka_host, self.redis_port, self.redis_db,
                                  password=self.redis_password, retries=self.kafka_retries,
                                  username=self.kafka_username,
                                  group=self.kafka_group)
            ret_code, ret_msg = self.storer.open()
            if not ret_code:
                print(ret_msg)
        else:
            self.storer = OFile(encoding='utf-8')
        return self.storer


class TestMdConfig(unittest.TestCase):
    def test_mdb(self):
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)

        md_config = MdConfig()
        rds = md_config.get_rds()
        print(rds.ping())
