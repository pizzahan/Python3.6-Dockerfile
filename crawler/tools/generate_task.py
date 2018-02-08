#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import re
import redis


def format_element(element):
    if not isinstance(element, str):
        return element
    result = re.sub('\s', '', element)
    result = re.sub('[\'\" \/]', '', result)
    result = re.sub('（', '(', result)
    result = re.sub('）', ')', result)
    idx = result.find('公司')
    if idx >= 0:
        result = result.split('公司')[0].strip() + '公司'
    return result


url_dic = {
    'download_itjuzi_company': 'https://www.itjuzi.com/company/',
    'download_itjuzi_company_page': 'https://www.itjuzi.com/company?page=',
    'download_itjuzi_company_listed_page': 'https://www.itjuzi.com/company/listed?page=',
    'download_itjuzi_company_foreign_page': 'https://www.itjuzi.com/company/foreign?page=',
    'download_itjuzi_company_foreign_listed_page': 'https://www.itjuzi.com/company/foreign/listed?page=',
    'analyze_itjuzi_company': 'https://www.itjuzi.com/company/',
    'analyze_itjuzi_company_page': 'https://www.itjuzi.com/company/?page=',
    'analyze_itjuzi_company_listed_page': 'https://www.itjuzi.com/company/listed?page=',
    'analyze_itjuzi_company_foreign_page': 'https://www.itjuzi.com/company/foreign?page=',
    'analyze_itjuzi_company_foreign_listed_page': 'https://www.itjuzi.com/company/foreign/listed?page=',
    'download_itjuzi_invest_page': 'https://www.itjuzi.com/investfirm?page=',
    'analyze_itjuzi_invest_page': 'https://www.itjuzi.com/investfirm?page=',
    'download_itjuzi_invest': 'http://radar.itjuzi.com/company/invest/',
    'analyze_itjuzi_invest': 'http://radar.itjuzi.com/company/invest/',
    'analyze_itjuzi_team': 'http://radar.itjuzi.com/company/team/',
    'analyze_itjuzi_invest_firm': 'https://www.itjuzi.com/investfirm/',
    'download_qichacha_search': 'http://www.qichacha.com/search?key=',
    'download_qichacha_company': 'http://www.qichacha.com/',
    'analyze_qichacha_company': 'http://www.jim.com/',
    'download_icp_beian': 'http://www.beianbeian.com/s?keytype=2&q=',
    'analyze_icp_beian': 'http://www.beianbeian.com/s?keytype=2&q=',
    'down_qichacha_search': 'http://www.qichacha.com/search?key=',
    'down_qichacha_company': 'http://www.qichacha.com/firm_',
    'down_qichacha_run': 'http://www.qichacha.com/crun_',
    'down_qichacha_assets': 'http://www.qichacha.com/cassets_',
    'download_weixin_search': 'http://weixin.sogou.com/weixin?type=1&query=',
    'download_sogou_search': 'https://www.sogou.com/web?query=',
    'download_baidu_search': 'https://www.baidu.com/s?wd=',
    'download_360_search': 'https://www.so.com/s?q=',
    'analyze_weixin_search': 'http://weixin.sogou.com/weixin?type=1&query=',
    'analyze_sogou_search': 'https://www.sogou.com/web?query=',
    'analyze_baidu_search': 'https://www.baidu.com/s?wd=',
    'analyze_360_search': 'https://www.so.com/s?q=',
}

down_pattern = re.compile(r'down')

if __name__ == "__main__":
    argc = len(sys.argv)
    if argc < 4:
        print("usage: python {0} file queue path".format(sys.argv[0]))
        sys.exit(-1)
    task_file = sys.argv[1]
    task_queue = sys.argv[2]
    task_path = sys.argv[3]

    rds = redis.Redis(host='127.0.0.1', port=21601, db=0, password='Mindata123', decode_responses=True)

    down_flag = down_pattern.match(task_queue)
    with open(task_file, encoding='utf-8') as fp:
        for line in fp:
            task_id = format_element(line.strip())
            if len(task_id) == 0:
                continue
            task_url = url_dic[task_queue] + task_id
            task_file = '{0}{1}.html'.format(task_path, task_id)
            if down_flag and os.path.exists(task_file):
                size = os.stat(task_file).st_size
                if size > 1024:
                    continue
            task = {'url': task_url, 'file': task_file, 'type': task_queue}
            content = json.dumps(task)
            # rds.rpush(task_queue, content)
            length = rds.rpush(task_queue, content)
            print('{0}\t{1}\t{2} '.format(length, task_url, task_file))
