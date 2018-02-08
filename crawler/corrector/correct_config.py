#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import redis
import pymysql
import configparser
import logging
import logging.config
import unittest


class CorrectConfig(object):
    def __init__(self, config):
        cf = configparser.ConfigParser()
        cf.read(config)

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


class TestCorrectConfig(unittest.TestCase):
    def test_mdb(self):
        conf_file = './conf/corrector.conf'
        log_conf = './conf/logging.conf'
        logging.config.fileConfig(log_conf)
        gain_config = CorrectConfig(conf_file)
        rds = gain_config.get_rds()
        print(rds.ping())
        mdb = gain_config.get_mdb()
        print(mdb)


if __name__ == '__main__':
    unittest.main()
