#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import redis
import requests
import time
import traceback
import logging
import logging.handlers

from ip2Region import Ip2Region

LOG_FILE = "./log/switch_ip.log"


def initlog():
    i_logger = logging.getLogger()
    hdlr = logging.handlers.TimedRotatingFileHandler(LOG_FILE, when='H', interval=1, backupCount=40)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    i_logger.addHandler(hdlr)
    i_logger.setLevel(logging.NOTSET)
    return i_logger


logger = initlog()

if __name__ == "__main__":
    redis_host = '127.0.0.1'
    redis_port = 21601
    redis_password = 'Mindata123'
    rds = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password, decode_responses=True)
    if not rds.ping():
        print('cannot connect rds')
        sys.exit(-1)

    proxy_key = 'abu_pro_proxy'
    proxy_host = rds.hget(proxy_key, 'host')
    proxy_port = rds.hget(proxy_key, 'port')
    proxy_user = rds.hget(proxy_key, 'user')
    proxy_password = rds.hget(proxy_key, 'password')

    proxy_meta = 'http://{0}:{1}@{2}:{3}'.format(proxy_user, proxy_password, proxy_host, proxy_port)

    proxies = {
        'http': proxy_meta,
        'https': proxy_meta,
    }

    switch_url = 'http://proxy.abuyun.com/switch-ip'
    searcher = Ip2Region('./ip2region.db')
    while True:
        time.sleep(1)
        try:
            resp = requests.get(switch_url, proxies=proxies)
            if resp.status_code == 200:
                infos = resp.text.split(',')
                ip = infos[0]
                ip_info = searcher.btreeSearch(ip)
                if 'region' in ip_info:
                    region = ip_info['region'].decode()
                    items = region.split('|')
                    country = items[0]
                    area = items[1]
                    province = items[2]
                    city = items[3]
                    ISP = items[4]
                    logger.info('{0} {1} {2} {3} {4} {5}'.format(ip, country, area, province, city, ISP))
                else:
                    logger.info(resp.text)
            else:
                logger.error('{0} {1}'.format(resp.status_code, resp.text))
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
