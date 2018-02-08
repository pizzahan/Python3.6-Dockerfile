#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from urllib import parse
import re
from gain_public import get_url_params


def itjuzi_split_colon(task, target_str):
    if 'type' in task:
        return target_str.split('：')[-1]
    return ''


def split_space(task, target_str):
    if 'type' in task:
        return target_str.split('\xa0')[-1]
    return ''


def itjuzi_get_id(task, target_str):
    id_str = ''
    if 'type' in task:
        if not isinstance(target_str, list):
            target_str = [target_str]
        ids = []
        for target in target_str:
            ids.append(target.split('/')[-1])
        id_str = ','.join(e for e in ids)
    return id_str


def connect_url_nextpage(task, target_str):
    if 'url' in task:
        parsedtub = parse.urlparse(task.get('url'))
        resstr = parse.urlunparse((parsedtub.scheme, parsedtub.netloc,
                                   parsedtub.path, parsedtub.params,
                                   target_str[1:], parsedtub.fragment))
        return resstr
    return ''


def connect_url_about(task, target_str):
    resstr = ''
    if 'type' in task:
        resstr = target_str + '/about'
    return resstr


def generate_radar_link(task, target_str):
    if 'type' in task:
        return 'http://radar.itjuzi.com/company/' + target_str.split('/')[-1]
    return ''


def generate_team_link(task, target_str):
    if 'type' in task:
        return 'http://radar.itjuzi.com/company/team/' + target_str.split('/')[-1]
    return ''


def generate_invest_link(task, target_str):
    if 'type' in task:
        return 'http://radar.itjuzi.com/company/invest/' + target_str.split('/')[-1]
    return ''


def generate_compete_link(task, target_str):
    if 'type' in task:
        return 'http://radar.itjuzi.com/company/compete/' + target_str.split('/')[-1]
    return ''


def generate_qichacha_link(task, target_str):
    if 'type' in task:
        return 'http://www.qichacha.com' + target_str
    return ''


def generate_qichacha_search(task, target_str):
    if 'type' in task:
        return 'http://www.qichacha.com/search?key=' + parse.quote(target_str)
    return ''


def generate_58companylist(task, target_str):
    if task.get('file'):
        page_num = int(target_str)
        reslist = []
        parsedtub = parse.urlparse(task.get('url'))
        if re.findall(u"[\u4e00-\u9fa5]+", task.get('file')):
            for page_index in range(2, page_num + 1):
                tmp_str = parse.urlunparse((parsedtub.scheme, parsedtub.netloc,
                                            '{0}pn{1}/'.format(parsedtub.path, page_index), parsedtub.params,
                                            parsedtub.query, parsedtub.fragment))
                reslist.append(tmp_str)
            return reslist
    return ''


def str_to_number(task, task_str):
    number = 0
    if 'type' in task:
        try:
            num_str = re.sub('\D', '', task_str)
            number = int(num_str)
        except Exception as e:
            print(e)
    return number


def get_qichacha_id(task, task_str):
    task_id = task_str
    if 'type' in task:
        try:
            info = task_str.split('/')[-1]
            item = info.split('_')[-1]
            task_id = item.split('.')[0]
        except Exception as e:
            print(e)
    return task_id


def get_baidu_word(task, task_str):
    word = ''
    if 'type' in task:
        params = get_url_params(task_str)
        word = params.get('wd', '').rstrip('@v')
    return word
