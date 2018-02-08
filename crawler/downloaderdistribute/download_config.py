#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
import configparser
import logging
import logging.config
import unittest

from download_mysql import DbMysql


class MdConfig(object):
    def __init__(self, config):
        cf = configparser.ConfigParser()
        cf.read(config)

        # redis config
        self.rds_host = cf.get('redis', 'host')
        self.rds_port = cf.getint('redis', 'port')
        self.rds_password = cf.get('redis', 'password')
        self.rds = redis.Redis(host=self.rds_host, port=self.rds_port, db=0, password=self.rds_password,
                               decode_responses=True)
        logging.info('redis host[{0}] port[{1}]'.format(self.rds_host, self.rds_port))

        # stop key
        self.run_seqs = cf.get('data', 'run_seqs')
        self.stop_key = cf.get('data', 'stop_key')
        self.data_path = cf.get('data', 'data_path')
        self.stat_hash = cf.get('data', 'stat_hash')
        self.proxy_key = cf.get('data', 'proxy_key')
        self.proxy_index = cf.get('data', 'proxy_index')
        self.lock_sesseion_sec = cf.get('data', 'lock_sesseion_sec')
        self.time_out = cf.getint('data', 'time_out')

        # proxy config
        self.proxy_host = self.rds.hget(self.proxy_key, 'host')
        self.proxy_port = self.rds.hget(self.proxy_key, 'port')
        self.proxy_user = self.rds.hget(self.proxy_key, 'user')
        self.proxy_password = self.rds.hget(self.proxy_key, 'password')
        logging.info('proxy host[{0}] port[{1}]'.format(self.proxy_host, self.proxy_port))

        # mysql config
        self.my_host = cf.get('mysql', 'host')
        self.my_port = cf.getint('mysql', 'port')
        self.my_user = cf.get('mysql', 'user')
        self.my_password = cf.get('mysql', 'password')
        self.my_db = cf.get('mysql', 'db')
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

        md_config = MdConfig(conf_file)
        rds = md_config.get_rds()
        print(rds.ping())


if __name__ == '__main__':
    unittest.main()
