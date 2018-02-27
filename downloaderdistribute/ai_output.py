#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import traceback

from ai_queue import RdsQueue, KafkaQueue
import unittest


class AIOStream(object):
    def __init__(self):
        pass

    def open(self):
        pass

    def write(self, key, message):
        pass

    def close(self):
        pass


class IOQueue(AIOStream):
    def __init__(self, queue_type, host, port, db, password=None, retries=3, username=None, group=None):
        super().__init__()
        if queue_type == 'redis':
            self.queue = RdsQueue(host=host, port=port, db=db, password=password)
        elif queue_type == 'kafka':
            self.queue = KafkaQueue(host=host, retries=retries, username=username, password=password, group=group)

    def open(self):
        try:
            res, msg = self.queue.Connect()
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def read(self, key):
        try:
            res, msg = self.queue.Lpop(key)
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def write(self, key, message):
        try:
            res, msg = self.queue.Rpush(key, message)
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def close(self):
        res = True
        msg = ''
        try:
            self.queue.Close()
        except Exception as e:
            print(traceback.format_exc())
            msg = '{0}'.format(e)
            res = False
        return res, msg


class OFile(AIOStream):
    def __init__(self, buffering=None, encoding=None, errors=None, newline=None, closefd=True):
        super().__init__()
        self.fb = None
        self.buffering = buffering
        self.encoding = encoding
        self.errors = errors
        self.newline = newline
        self.closefd = closefd

    def open(self):
        pass

    def write(self, key, message):
        res = True
        msg = ''
        try:
            if os.path.exists(key):
                self.fb = open(key, mode='a', encoding=self.encoding, newline=self.newline)
            else:
                self.fb = open(key, mode='w', encoding=self.encoding, newline=self.newline)
            if len(message) > 0:
                message = message.strip()
                self.fb.write(message)
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg

    def close(self):
        res = True
        msg = ''
        try:
            self.fb.close()
        except Exception as e:
            msg = '{0}'.format(e)
            res = False
        return res, msg


class TestOFile(unittest.TestCase):
    def test_OFile(self):
        filename = '~/test_output.txt'
        encoding = 'utf-8'
        message = '万里长城今犹在，'
        ofiler = OFile(encoding=encoding)
        ofiler.write(filename, message)
        ofiler.close()


class TestIOQueue(unittest.TestCase):
    def test_IOQueue(self):
        queue_type = 'redis'
        in_key = 'tmp_key'
        host = '192.168.1.205'
        port = 21602
        db = 0
        password = 'Mindata123'
        retries = 3
        username = None
        group = None
        message = '万里长城今犹在，'
        try:
            oqueuer = IOQueue(queue_type, host, port, db, password=password, retries=retries, username=username,
                              group=group)
            ret_code, ret_msg = oqueuer.open()
            if not ret_code:
                print(ret_msg)
                return
            ret_code, ret_msg = oqueuer.write(in_key, message)
            if not ret_code:
                print(ret_msg)
                return
            ret_code, ret_msg = oqueuer.read(in_key)
            if not ret_code:
                print(ret_msg)
                return
            ret_code, ret_msg = oqueuer.close()
            if not ret_code:
                print(ret_msg)
                return
        except Exception as e:
            print(e)

    def test_kafkaQueue(self):
        queue_type = 'kafka'
        in_key = 'strategy-push'
        host = '192.168.1.205:9092,192.168.1.206:9092,192.168.1.207:9092'
        port = 21602
        db = 0
        password = 'Mindata123'
        retries = 3
        username = None
        group = None
        message = 'test_2'
        try:
            oqueuer = IOQueue(queue_type, host, port, db, password=password, retries=retries, username=username,
                              group=group)
            ret_code, ret_msg = oqueuer.open()
            if not ret_code:
                print(ret_msg)
                return
            #ret_code, ret_msg = oqueuer.write(in_key, message)
            if not ret_code:
                print(ret_msg)
                return
            ret_code, ret_msg = oqueuer.read(in_key)
            if not ret_code:
                print(ret_msg.value.decode('utf-8'))
                return
            print(ret_msg)
            ret_code, ret_msg = oqueuer.close()
            if not ret_code:
                print(ret_msg)
                return
        except Exception as e:
            print(e)
