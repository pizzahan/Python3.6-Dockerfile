#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse
import time

import logging
import logging.config

from download_config import MdConfig
from download_thread import MdThread


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='conf/downloader.conf')
    parser.add_argument('--logging', help='日志配置', default='conf/logging.conf')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)

    config = MdConfig(args.config)
    abu_thread = MdThread(config)
    abu_thread.run()
    abu_thread.join()
    logging.info('downloader exit.')


if __name__ == '__main__':
    # unittest.main()
    main()