#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import argparse

import logging
import logging.config

from gain_config import GaInConfig
from gain_thread import GaInThread


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', help='配置文件', default='conf/analyzer.conf')
    parser.add_argument('--logging', help='日志配置', default='conf/logging.conf')
    args = parser.parse_args()

    if args.logging:
        os.makedirs('log', exist_ok=True)
        logging.config.fileConfig(args.logging)

    config = GaInConfig(args.config)
    gain_thread = GaInThread(config)
    gain_thread.run()
    gain_thread.join()
    logging.info('gain_main exit.')


if __name__ == '__main__':
    # unittest.main()
    main()
