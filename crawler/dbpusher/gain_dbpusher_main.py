#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse

import logging
import logging.config

from dbpusher_config import DbConfig
from dbpusher_thread import DbThread


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='conf/dbpusher.conf')
    parser.add_argument('--logging', help='日志配置', default='conf/logging.conf')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)

    config = DbConfig(args.config)
    gain_thread = DbThread(config)
    gain_thread.run()
    gain_thread.join()
    logging.info('dbpusher main thread exit.')


if __name__ == '__main__':
    # unittest.main()
    main()
