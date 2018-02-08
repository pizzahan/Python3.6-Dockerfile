#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import redis
import pymysql
import configparser
import logging
import logging.config
import unittest


class GaInConfig(object):
    def __init__(self, config):
        cf = configparser.ConfigParser()
        cf.read(config)
        self.out_flag = cf.get('out', 'out_flag')
        self.out_path = cf.get('out', 'out_path')

        # redis config
        self.rds_host = cf.get('redis', 'host')
        self.rds_port = cf.getint('redis', 'port')
        self.rds_password = cf.get('redis', 'password')
        self.rds = redis.Redis(host=self.rds_host,
                               port=self.rds_port,
                               db=0,
                               password=self.rds_password,
                               decode_responses=True
                               )
        logging.info('redis host[{0}] port[{1}]'.format(self.rds_host, self.rds_port))

        # stop key
        self.run_seqs = cf.get('data', 'run_seqs')
        self.stop_key = cf.get('data', 'stop_key')
        self.stat_hash = cf.get('data', 'stat_hash')
        # mysql config
        self.my_host = cf.get('mysql', 'host')
        self.my_port = cf.getint('mysql', 'port')
        self.my_user = cf.get('mysql', 'user')
        self.my_password = cf.get('mysql', 'password')
        self.my_db = cf.get('mysql', 'db')
        self.mdb = None
        logging.info('mysql host[{0}] port[{1}]'.format(self.my_host, self.my_port))

    def get_rds(self):
        return self.rds

    def get_mdb(self):
        if self.mdb is None:
            try:
                dbconn = pymysql.connect(host=self.my_host,
                                         user=self.my_user,
                                         passwd=self.my_password,
                                         port=self.my_port,
                                         db=self.my_db,
                                         charset="utf8")
                self.mdb = dbconn
            except Exception as err:
                logging.error('get_mdb_conn connect error:{0}'.format(err))
        # logging.info('get_mdb_conn {0}'.format(self.mdb))
        return self.mdb

    def new_mdb(self):
        try:
            dbconn = pymysql.connect(host=self.my_host,
                                     user=self.my_user,
                                     passwd=self.my_password,
                                     port=self.my_port,
                                     db=self.my_db,
                                     charset="utf8")
            self.mdb = dbconn
        except Exception as err:
            logging.error('new_mdb connect error:{0}'.format(err))
        return self.mdb

    def commit(self):
        self.mdb.commit()


class TestGaInConfig(unittest.TestCase):
    @staticmethod
    def init():
        log_conf = './log.conf'
        log_file = './test.log'
        conf_file = './config.conf'
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
            fp.write('run_seqs = 0,2\n')
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
        log_file = './test.log'
        conf_file = './config.conf'
        os.remove(conf_file)
        os.remove(log_conf)
        os.remove(log_file)

    def test_mdb(self):
        conf_file = self.init()

        gain_config = GaInConfig(conf_file)
        rds = gain_config.get_rds()
        print(rds.ping())
        gain_config.get_mdb()
        print(gain_config.mdb)

        self.clear()


if __name__ == '__main__':
    unittest.main()
