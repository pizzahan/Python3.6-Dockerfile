#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import traceback
import logging
import json
from lxml import etree

from gain_define import *

consts = {
    'normal_element_type': 0,
    'check_element_type': 1,
    'store_element_type': 2,
    'store_unit_type': 3
}


# 无用，只是为了pycharm校验通过（from gain_define import *）
def define_demo(task, target_str):
    return itjuzi_split_colon(task, target_str)


class ElementExtractor(object):
    def __init__(self):
        self.store_element = ''

    # 读取mysql任务表有效的任务信息
    @staticmethod
    def get_tasks(connection, task_seqs):
        tasks = {}
        with connection.cursor() as cursor:
            try:
                sql = '''select task, thread_num, valid_size, exception_queue,
                        next_task, store_hash
                        from scrapy_task where status=1 and seq in ({0})'''.format(task_seqs)
                cursor.execute(sql)
                rows = cursor.fetchall()
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                return tasks

            for row in rows:
                task, thread_num, valid_size, exception_queue, next_task, store_hash = row[:]
                tasks[task] = [thread_num, valid_size, exception_queue, next_task, store_hash]
        return tasks

    @staticmethod
    def get_rules(connection, task):
        rules = {}
        with connection.cursor() as cursor:
            try:
                sql = '''select seq, element_type, element, element_xpath,
                        element_subpath, notify_queue, `action`, element_index,
                        store_hash
                        from scrapy_rule where task='{0}'
                    '''.format(task)
                cursor.execute(sql)
                logging.info(sql)
                rows = cursor.fetchall()
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                return rules

            for row in rows:
                seq = row[0]
                element_type = row[1]
                element = row[2]
                element_xpath = row[3]
                element_subpath = row[4]
                notify_queue = row[5]
                action = row[6]
                element_index = row[7]
                store_hash = row[8]
                rules.setdefault(seq, {})
                rules[seq].setdefault(element_type, {})
                rules[seq][element_type].setdefault(element_xpath, {})
                rules[seq][element_type][element_xpath][element] = [element_subpath, notify_queue, action,
                                                                    element_index,
                                                                    store_hash]
        return rules

    # 校验元素, 所有的校验元素都通过才算通过  element_type: 0普通元素 1校验预算 2保存键值元素
    @staticmethod
    def check_element(selector, rules):
        ok_seq = 0
        ok_flag = False
        msg = ''
        for seq in rules:
            if consts['check_element_type'] in rules[seq]:
                check_elements = rules[seq][consts['check_element_type']]
                logging.info(check_elements)
                check_flag = True
                for element_xpath in check_elements:
                    contents = selector.xpath(element_xpath)
                    # 一个失败即退出
                    if len(contents) == 0:
                        check_flag = False
                        logging.warning('rules {0} check failed, no element {1}'.format(seq, element_xpath))
                        break
                if check_flag:
                    ok_seq = seq
                    ok_flag = True
                    break
            else:
                ok_seq = seq
                ok_flag = True
                break
        if not ok_flag:
            msg = 'no valid element {0}'.format(rules)
        return ok_seq, msg

    @staticmethod
    def format_element(element):
        if not isinstance(element, str):
            return element
        result = element.strip()
        idx = result.find('\t')
        if idx >= 0:
            result = result.replace('\t', '')
        idx = result.find('\n')
        if idx >= 0:
            result = result.replace('\n', '')
        idx = result.find('\'')
        if idx >= 0:
            result = result.replace('\'', '^')
        idx = result.find('\"')
        if idx >= 0:
            result = result.replace('\"', '')
        return result.strip()

    def act_element_value(self, task, action, target):
        if action == '':
            return target
        if isinstance(target, list):
            results = []
            for item in target:
                func = eval(action)
                if hasattr(func, '__call__'):
                    values = func(task, item)
                else:
                    values = func
                if isinstance(values, list):
                    for value in values:
                        result = self.format_element(value)
                        results.append(result)
                else:
                    result = self.format_element(values)
                    results.append(result)
        else:
            func = eval(action)
            if hasattr(func, '__call__'):
                values = func(task, target)
            else:
                values = func
            if isinstance(values, list):
                results = []
                for value in values:
                    result = self.format_element(value)
                    results.append(result)
            else:
                results = self.format_element(values)
        return results

    # 根据xpath提取内容
    def get_element(self, selector, element_xpath, element_subpath):
        values = ''
        contents = []
        try:
            contents = selector.xpath(element_xpath)
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            logging.error('deal xpath {0} subxpath {1} failed'.format(element_xpath, element_subpath))
        if len(contents) == 0:
            return values
        if element_subpath != '':
            for content in contents:
                sub_contents = content.xpath(element_subpath)
                if isinstance(sub_contents, list):
                    values = []
                    for sub_content in sub_contents:
                        result = self.format_element(sub_content)
                        if len(result) != 0:
                            values.append(result)
                else:
                    values = self.format_element(sub_contents)
        else:
            if isinstance(contents, list):
                values = []
                for content in contents:
                    result = self.format_element(content)
                    if len(result) != 0:
                        values.append(result)
            else:
                values = self.format_element(contents)
        return values

    def get_element_value(self, results, element_index):
        element_value = ''
        if results is None:
            return element_value
        if not isinstance(results, int) and len(results) == 0:
            return element_value
        if isinstance(results, list):
            if element_index != -1:
                if len(results) > element_index:
                    element_value = self.format_element(results[element_index])
            else:
                element_value = []
                for result in results:
                    value = self.format_element(result)
                    if len(value) > 0:
                        element_value.append(value)
        else:
            element_value = self.format_element(results)
        return element_value

    def get_store_field(self, selector, task, task_rules, seq):
        store_field = ''
        if not consts['store_element_type'] in task_rules[seq]:
            return store_field
        store_elements = task_rules[seq][consts['store_element_type']]
        join_list = []
        for element_xpath in store_elements:
            parents = selector.xpath(element_xpath)
            if len(parents) == 0:
                continue
            if not isinstance(parents, list):
                parents = [parents]
            for parent in parents:
                for element in store_elements[element_xpath]:
                    element_subpath = store_elements[element_xpath][element][0]
                    action = store_elements[element_xpath][element][2]
                    element_index = store_elements[element_xpath][element][3]
                    results = parents
                    if element_subpath != '':
                        results = self.get_element(parent, element_subpath, '')
                    results = self.act_element_value(task, action, results)
                    store_value = self.get_element_value(results, element_index)
                    if store_value is None or len(store_value) == 0:
                        continue
                    if isinstance(store_value, list):
                        for value in store_value:
                            join_list.append(value)
                    else:
                        join_list.append(store_value)
        if len(join_list) != 0:
            store_field = '_'.join(e for e in join_list)
        return store_field

    # 解析html文件元素
    def get_html_element(self, rds_cli, rules, task, task_info_list):
        task_type = task['type']
        if task_type not in rules:
            logging.error('task {0} rule not in rules {1}'.format(task_type, rules))
            return -1, {}
        task_rules = rules[task_type]
        task_file = task['file']
        if not os.path.exists(task_file):
            logging.error('task_file {0} not found.'.format(task_file))
            return -1, {}

        try:
            selector = etree.HTML(open(task_file, encoding='utf-8').read())
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            return -2, {}

        # 校验元素
        seq, msg = self.check_element(selector, task_rules)
        if msg != '':
            logging.error('check task[{0}] failed'.format(task))
            return -3, {}

        # 普通元素  嵌套 seq-》type-》xpath-》element = [sub_xpath, notify, action, index, hash]
        normal_elements = task_rules[seq][consts['normal_element_type']]
        elements = {}
        for element_xpath in normal_elements:
            parents = []
            try:
                parents = selector.xpath(element_xpath)
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                logging.error('deal task {0} xpath {1} failed'.format(task, element_xpath))
            if len(parents) == 0:
                continue
            if not isinstance(parents, list):
                parents = [parents]

            # 相同xpath下多个supxpath, 结果是列表,想要以整个单元存储, 需要一个指定名称
            store_unit_xpath = ''
            if consts['store_unit_type'] in task_rules[seq] \
                    and element_xpath in task_rules[seq][consts['store_unit_type']]:
                for element in task_rules[seq][consts['store_unit_type']][element_xpath]:
                    store_unit_xpath = element
                    break

            for parent in parents:
                # 遍历相同xpath下的各个元素
                element_dic = {}
                for element in normal_elements[element_xpath]:
                    element_subpath = normal_elements[element_xpath][element][0]
                    notify_queue = normal_elements[element_xpath][element][1]
                    action = normal_elements[element_xpath][element][2]
                    element_index = normal_elements[element_xpath][element][3]
                    results = parents
                    if element_subpath != '':
                        results = self.get_element(parent, element_subpath, '')
                    results = self.act_element_value(task, action, results)
                    element_value = self.get_element_value(results, element_index)
                    if element_value is None:
                        logging.warning('element {0} not found in task {1}'.format(element, task))
                        continue
                    if not isinstance(element_value, int) and len(element_value) == 0:
                        logging.warning('element {0} is empty in task {1}'.format(element, task))
                        continue

                    # 直接保存还是先存信息单元
                    if store_unit_xpath == '':
                        elements[element] = element_value
                    else:
                        element_dic[element] = element_value

                    if notify_queue != '':
                        next_task = {'type': notify_queue}
                        if not isinstance(element_value, list):
                            notify_list = [element_value]
                        else:
                            notify_list = element_value
                        for value in notify_list:
                            try:
                                next_task['url'] = value
                                resp = rds_cli.rpush(notify_queue, json.dumps(next_task))
                                if resp is None:
                                    logging.warning(
                                        'rpush queue[{0}][{1}] unexpect response'.format(notify_queue, next_task))
                                else:
                                    logging.info('rpush queue {0} {1}'.format(notify_queue, next_task))
                            except Exception as e:
                                logging.error(e)
                                logging.error(traceback.format_exc())
                                logging.error(task)

                if store_unit_xpath != '':
                    elements.setdefault(store_unit_xpath, [])
                    elements[store_unit_xpath].append(element_dic)
        self.store_element = self.get_store_field(selector, task, task_rules, seq)
        return 0, elements
