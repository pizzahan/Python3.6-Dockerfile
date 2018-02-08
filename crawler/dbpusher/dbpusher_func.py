#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging
import json
import traceback


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
            return length
        else:
            logging.error('queue {0} is empty'.format(queue))
            return -1
    except Exception as e:
        logging.error('rds {0} push to queue {1} msg {2} failed[{3}]'.format(rds, queue, msg, e))
        return -1


# 读取mysql任务表有效的任务信息
def get_tasks(db_mdb, run_seqs):
    tasks = {}
    try:
        sql = '''select task, task_function, task_table, task_hash from scrapy_db_load
                where status=1 and seq in ({0})'''.format(run_seqs)
        rows = db_mdb.query(sql)
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        return tasks

    for row in rows:
        task, task_function, task_table, task_hash = row[:]
        tasks.setdefault(task, {})
        tasks[task][task_function] = [task_table, task_hash]
    return tasks


# 读取入库函数配置
def get_funcs(db_mdb, func_list):
    results = {}
    func_str = "','".join(e for e in func_list)
    try:
        sql = '''select table_function, field_type, field, sub_field, column_name, column_type
                from scrapy_db_function where table_function in ('{0}') and status=1'''.format(func_str)
        rows = db_mdb.query(sql)
    except Exception as e:
        logging.error(e)
        logging.error(traceback.format_exc())
        return results

    for row in rows:
        func, field_type, field, sub_field, column_name, column_type = row[:]
        results.setdefault(func, {})
        results[func].setdefault(field_type, {})
        results[func][field_type][column_name] = [field, sub_field, column_type]
    return results


def get_field_value(values, field, column_type):
    default = ''
    if column_type == 1:
        default = '0'
    value = values.get(field, default)
    if isinstance(value, list):
        value = ','.join(str(e) for e in value)
    elif value == '' and column_type == 1:
        value = '0'
    return value


def get_field_values(values, field, sub_field, column_type):
    results = []
    value = values.get(field, '')
    if isinstance(value, list):
        for item in value:
            result = get_field_value(item, sub_field, column_type)
            results.append(result)
    else:
        results.append(value)
    return results


def get_column_values(values, func_infos, field_type):
    column_values = {}
    cycle = 0
    if field_type not in func_infos:
        return column_values, cycle
    cycle = 1
    columns = func_infos[field_type].keys()
    for column in columns:
        field, sub_field, column_type = func_infos[field_type][column]
        if sub_field == '':
            column_values[column] = [get_field_value(values, field, column_type)]
        else:
            column_values[column] = get_field_values(values, field, sub_field, column_type)
            if cycle < len(column_values[column]):
                cycle = len(column_values[column])
    return column_values, cycle


def generate_insert_sqls(table, func_infos, column_values, cycle):
    columns = column_values.keys()
    insert_field_str = '`, `'.join(columns)
    i = 0
    sqls = []
    while i < cycle:
        value_str = ''
        for column in columns:
            idx = min(i, len(column_values[column]) - 1)
            column_type = func_infos[0][column][2]
            if column_type == 1:
                if len(column_values[column][idx]) == 0:
                    column_values[column][idx] = 0
                value_str += ', {0}'.format(column_values[column][idx])
            else:
                value_str += ", '{0}'".format(column_values[column][idx])
        value_str = value_str.strip(', ')
        sql = '''insert into {0}(`{1}`) values({2})'''.format(table, insert_field_str, value_str)
        sqls.append(sql)
        i += 1
    return sqls


def generate_insert_update_sqls(table, func_infos, column_values, cycle, update_values):
    columns = column_values.keys()
    insert_field_str = '`, `'.join(columns)
    i = 0
    sqls = []
    while i < cycle:
        value_str = ''
        for column in columns:
            idx = min(i, len(column_values[column]) - 1)
            column_type = func_infos[0][column][2]
            if column_type == 1:
                value_str += ', {0}'.format(column_values[column][idx])
            else:
                value_str += ", '{0}'".format(column_values[column][idx])
        value_str = value_str.strip(', ')
        update_str = ' '
        for column in update_values.keys():
            idx = min(i, len(update_values[column]) - 1)
            column_type = func_infos[1][column][2]
            if column_type == 1:
                update_str += '`{0}`={1}, '.format(column, update_values[column][idx])
            else:
                update_str += "`{0}`='{1}', ".format(column, update_values[column][idx])
        update_str = update_str.strip(', ')
        sql = '''insert into {0}(`{1}`) values({2}) on duplicate key update {3}
                '''.format(table, insert_field_str, value_str, update_str)
        sqls.append(sql)
        i += 1
    return sqls


def generate_update_sqls(table, func_infos, update_values, cycle, where_values):
    columns = update_values.keys()
    i = 0
    sqls = []
    while i < cycle:
        value_str = 'set '
        for column in columns:
            idx = min(i, len(update_values[column]) - 1)
            column_type = func_infos[1][column][2]
            if column_type == 1:
                value_str += '`{0}`={1}, '.format(column, update_values[column][idx])
            else:
                value_str += "`{0}`='{1}', ".format(column, update_values[column][idx])
        value_str = value_str.strip(', ')
        where_str = ''
        for column in where_values.keys():
            idx = min(i, len(where_values[column]) - 1)
            column_type = func_infos[2][column][2]
            if column_type == 1:
                value_str += '`{0}`={1} and '.format(column, update_values[column][idx])
            else:
                value_str += "`{0}`='{1}' and".format(column, update_values[column][idx])
        sql = '''update {0} {1} where {2}'''.format(table, value_str, where_str[:-4])
        sqls.append(sql)
        i += 1
    return sqls


def deal_task(db_mdb, rds, task_id, task_infos, func_infos):
    status = -1
    for func in func_infos:
        if func not in task_infos:
            logging.warning('func {0} not config in scrapy_db_load'.format(func))
            continue
        table, store_hash = task_infos[func]
        value = rds.hget(store_hash, task_id)
        if not value:
            logging.warning('{0} {1} has no field data'.format(store_hash, task_id))
            continue
        values = json.loads(value)
        # 0 insert 1 update 2 where
        insert_values, insert_cycle = get_column_values(values, func_infos[func], 0)
        update_values, update_cycle = get_column_values(values, func_infos[func], 1)
        where_values, where_cycle = get_column_values(values, func_infos[func], 2)
        sqls = []
        if insert_cycle != 0:
            if update_cycle == 0:
                sqls = generate_insert_sqls(table, func_infos[func], insert_values, insert_cycle)
            elif update_cycle == insert_cycle:
                sqls = generate_insert_update_sqls(table, func_infos[func], insert_values, insert_cycle, update_values)
            else:
                logging.error('func {0} insert different update in {1} {2}'.format(func, store_hash, task_id))
        elif update_cycle != 0:
            if where_cycle != 0 and where_cycle == update_cycle:
                sqls = generate_update_sqls(table, func_infos[func], update_values, update_cycle, where_values)
            else:
                logging.error('func {0} update different where in {1} {2}'.format(func, store_hash, task_id))
        logging.info('sqls {0}'.format(sqls))
        status = db_mdb.execute(sqls)
    return status
