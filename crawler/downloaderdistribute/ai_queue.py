#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import sys
import redis
import time
from kafka import KafkaProducer
from kafka import KafkaConsumer
import unittest
import traceback


class AIQueue(object):
    def __init__(self):
        pass

    def Connect(self):
        pass

    def Rpush(self, queue, message):
        pass

    def Lpop(self, queue):
        pass

    def Close(self):
        pass


class RdsQueue(AIQueue):
    def __init__(self, host='localhost', port=6379, db=0, password=None):
        super().__init__()
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.rds = None

    def Connect(self):
        res = True
        msg = ''
        if self.rds is None:
            try:
                self.rds = redis.Redis(host=self.host, port=self.port, db=self.db, password=self.password,
                                       decode_responses=True)
                result = self.rds.ping()
                msg = '{0}'.format(result)
                if result != 'PONG':
                    res = False
            except Exception as e:
                msg = '{0}'.format(e)
                res = False
        return res, msg

    def Rpush(self, queue, message):
        res = True
        msg = ''
        try:
            result = self.rds.rpush(queue, message)
            msg = '{0}'.format(result)
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def Lpop(self, queue):
        res = True
        msg = ''
        try:
            result = self.rds.lpop(queue)
            msg = '{0}'.format(result)
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res,msg

    def Close(self):
        return True,''


class KafkaQueue(AIQueue):
    def __init__(self, host='localhost:9092', retries=3, username=None, password=None, group=None):
        super().__init__()
        self.hosts = host.split(',')
        self.username = username
        self.password = password
        self.retries = retries
        if group is not None:
            self.group = group
        else:
            self.group = 'default_consumer'
        self.producer = None
        self.consumers = {}
        self.yields = {}

    def Connect(self):
        res = True
        msg = ''
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.hosts,
                sasl_plain_username=self.username,
                sasl_plain_password=self.password,
                retries=self.retries
            )
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def Rpush(self, queue, message):
        res = True
        msg = ''
        try:
            result = self.producer.send(queue, value=message)
        except Exception as e:
            print(traceback.format_exc())
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def getYieldMsg(self, queue):
        if queue not in self.consumers.keys():
            consumer = KafkaConsumer(
                queue,
                group_id=self.group,
                bootstrap_servers=self.hosts,
                sasl_plain_username=self.username,
                sasl_plain_password=self.password,
            )
            consumer.close()
            self.consumers[queue] = consumer
        try:
            for message in self.consumers[queue]:
                yield message
        except Exception as e:
            print(e)

    def Lpop(self, queue):
        res = True
        msg = ''
        if queue not in self.yields.keys():
            self.yields[queue] = self.getYieldMsg(queue)
        try:
            msg = self.yields[queue].__next__()
        except Exception as e:
            msg = '{0}'.format(e)
            self.yields[queue] = self.getYieldMsg(queue)
            res = False
        return res, msg

    def Close(self):
        self.producer.close()
        for queue in self.consumers.keys():
            self.consumers[queue].close()


class TestRdsQueue(unittest.TestCase):
    def test_rds(self):
        host = '127.0.0.1'
        port = 21601
        password = 'Mindata123'
        db = 0
        rds = RdsQueue(host=host, port=port, db=db, password=password)
        res, msg = rds.Connect()
        if not res:
            print('rds connect failed, [{0}]'.format(msg))
            sys.exit(-1)
        res, msg = rds.Rpush('TestRdsQueue', 'hello, this is a TestRdsQueue msg')
        if msg != '':
            print('rds Rpush failed, [{0}]'.format(msg))
            sys.exit(-1)
        else:
            print('rds Rpush ok, [{0}]'.format(res))
        res = rds.Lpop('TestRdsQueue')
        print('rds Lpop [{0}]'.format(res))


class TestKafkaQueue(unittest.TestCase):
    def test_kafka(self):
        kfk = KafkaQueue()
        res, msg = kfk.Connect()
        if not res:
            print('kafka connect failed, [{0}]'.format(msg))
            sys.exit(-1)
        res, msg = kfk.Rpush('TestKafkaQueue', 'hello, this is a TestRdsQueue msg')
        if msg != '':
            print('kafka Rpush failed, [{0}]'.format(msg))
            sys.exit(-1)
        else:
            print('kafka Rpush ok, [{0}]'.format(res))
        res = kfk.Lpop('TestRdsQueue')
        print('kafka Lpop [{0}]'.format(res))
        time.sleep(10)
        kfk.Close()


if __name__ == '__main__':
    unittest.main()
