#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
import configparser
import logging
import logging.config
import traceback
import unittest

from dbpusher_mysql import DbMysql


class DbConfig(object):
    def __init__(self, config):
        cf = configparser.ConfigParser()
        cf.read(config)

        # redis config
        self.rds_host = cf.get('redis', 'host')
        self.rds_port = cf.getint('redis', 'port')
        self.rds_password = cf.get('redis', 'password')
        self.rds = None

        # stop key
        self.run_seqs = cf.get('data', 'run_seqs')
        self.stop_key = cf.get('data', 'stop_key')

        # mysql config
        self.my_host = cf.get('mysql', 'host')
        self.my_port = cf.getint('mysql', 'port')
        self.my_user = cf.get('mysql', 'user')
        self.my_password = cf.get('mysql', 'password')
        self.my_db = cf.get('mysql', 'db')

    # redis 多线程共用redis连接
    def get_rds(self):
        if self.rds is None:
            try:
                self.rds = redis.Redis(host=self.rds_host,
                                       port=self.rds_port,
                                       db=0,
                                       password=self.rds_password,
                                       decode_responses=True
                                       )
                logging.info('redis host[{0}] port[{1}]'.format(self.rds_host, self.rds_port))
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
        return self.rds

    # 每个线程一个数据库连接
    def get_mdb(self):
        mdb = DbMysql(self.my_host, self.my_port, self.my_user, self.my_password, self.my_db)
        return mdb


class TestDbConfig(unittest.TestCase):
    def test_config(self):
        conf_file = './conf/dbpusher.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)
        gain_config = DbConfig(conf_file)
        rds = gain_config.get_rds()
        print(rds.ping())
        mdb = gain_config.get_mdb()
        print(mdb.query('select now() as Systemtime'))


if __name__ == '__main__':
    unittest.main()
