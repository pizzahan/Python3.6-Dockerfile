#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import redis
import logging
import logging.config
import unittest

from download_mysql import DbMysql


class MdConfig(object):
    def __init__(self):
        env_idc = os.environ
        # redis config
        self.rds_host = env_idc.get('redis_host')
        self.rds_port = env_idc.getint('redis_port')
        self.rds_password = env_idc.get('redis_password')
        self.rds = redis.Redis(host=self.rds_host, port=self.rds_port, db=0, password=self.rds_password,
                               decode_responses=True)
        logging.info('redis host[{0}] port[{1}]'.format(self.rds_host, self.rds_port))

        # stop key
        self.run_seqs = env_idc.get('download_run_seqs')
        self.stop_key = env_idc.get('download_stop_key')
        self.data_path = env_idc.get('download_data_path')
        self.stat_hash = env_idc.get('download_stat_hash')
        self.proxy_key = env_idc.get('download_proxy_key')
        self.proxy_index = env_idc.get('download_proxy_index')
        self.lock_sesseion_sec = env_idc.get('download_lock_sesseion_sec')
        self.time_out = env_idc.getint('download_time_out')

        # proxy config
        self.proxy_host = self.rds.hget(self.proxy_key, 'host')
        self.proxy_port = self.rds.hget(self.proxy_key, 'port')
        self.proxy_user = self.rds.hget(self.proxy_key, 'user')
        self.proxy_password = self.rds.hget(self.proxy_key, 'password')
        logging.info('proxy host[{0}] port[{1}]'.format(self.proxy_host, self.proxy_port))

        # mysql config
        self.my_host = env_idc.get('mysql_host')
        self.my_port = env_idc.getint('mysql_port')
        self.my_user = env_idc.get('mysql_user')
        self.my_password = env_idc.get('mysql_password')
        self.my_db = env_idc.get('mysql_db')
        self.mdb = DbMysql(self.my_host, self.my_port, self.my_user, self.my_password, self.my_db)
        logging.info('mysql host[{0}] port[{1}]'.format(self.my_host, self.my_port))

    # redis线程安全, 共用一个连接即可, mysql不行
    def get_rds(self):
        return self.rds

    def get_mdb(self):
        return self.mdb


class TestMdConfig(unittest.TestCase):
    def test_mdb(self):
        conf_file = './conf/downloader.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)

        md_config = MdConfig()
        rds = md_config.get_rds()
        print(rds.ping())


if __name__ == '__main__':
    unittest.main()