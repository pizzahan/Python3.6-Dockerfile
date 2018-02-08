#!/usr/bin/env python3
# coding=utf-8

import os
import time
import re
from urllib import parse
import logging


def bkdr_hash(key):
    seed = 131  # 31 131 1313 13131 131313 etc..
    hash_zero = 0
    for i in range(len(key)):
        hash_zero = (hash_zero * seed) + ord(key[i])
    return hash_zero & 0x7FFFFFFF


def check_path(task_file):
    index = task_file.rfind("/")
    if index == -1:
        index = task_file.rfind("\\")
        if index == -1:
            return
    path = task_file[:index]
    if not os.path.exists(path):
        os.makedirs(path)
    return


# 判断队列长度,如果超过监控长度,暂停写入
def push(rds, queue, msg):
    try:
        if queue:
            while 1:
                length = rds.llen(queue)
                if length < 1000000:
                    break
                time.sleep(1)
            length = rds.rpush(queue, msg)
            return length, ''
        else:
            return 1, ''
    except Exception as e:
        return 0, '{0}'.format(e)


# 记录redis指标
def incr(rds, stor_hash, field_queue, field_type, number=1):
    cur_time = time.strftime("%Y%m%d%H", time.localtime())
    field = '{0}|{1}|{2}|{3}'.format(cur_time[:8], cur_time[8:10], field_queue, field_type)
    try:
        rds.hincrby(stor_hash, field, number)
    except Exception as e:
        logging.error(e)
        logging.error('incr {0} {1} {2} failed'.format(stor_hash, field, number))


# 批量替换
def multiple_replace(text, adict):
    rx = re.compile('|'.join(map(re.escape, adict)))

    def one_xlat(match):
        return adict[match.group(0)]

    return rx.sub(one_xlat, text)


# 获取url参数
def get_url_params(url):
    query = parse.urlparse(url).query
    return dict([(k, v[0]) for k, v in parse.parse_qs(query).items()])
