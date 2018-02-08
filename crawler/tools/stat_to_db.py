#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import socket
import redis
import pymysql
import traceback


def get_mdb_conn(my_host, my_user, my_password, my_port, my_db):
    dbconn = None
    ok_flag = True
    msg_sb = ''
    try:
        dbconn = pymysql.connect(
                host=my_host,
                user=my_user,
                passwd=my_password,
                port=my_port,
                db=my_db,
                charset="utf8")
    except Exception as err:
        ok_flag = False
        msg_sb = 'get_mdb_conn connect error:{0}'.format(err)
    return dbconn, ok_flag, msg_sb


def execute_sql(db_cursor, sql):
    try:
        db_cursor.execute(sql)
    except Exception as e:
        if e.args[0] != 1062:
            print(sql)
            print(traceback.format_exc())
    return


if __name__ == "__main__":
    # argc = len(sys.argv)
    # if 2 != argc:
    #    print('usage: python {0} table redis_hash'.format(sys.argv[0]))
    #    sys.exit(-1)

    redis_host = '127.0.0.1'
    redis_port = 21601
    redis_password = 'Mindata123'
    m_rds = redis.Redis(host=redis_host, port=redis_port, db=0, password=redis_password, decode_responses=True)
    if not m_rds.ping():
        print('cannot connect rds')
        sys.exit(-1)

    tz_host = '172.26.249.246'
    tz_port = 3306
    tz_user = 'md'
    tz_password = 'maida6868'
    tz_db = 'company'
    mdb, ok, m_msg = get_mdb_conn(tz_host, tz_user, tz_password, tz_port, tz_db)
    if not ok:
        print('invalid mysql {0}'.format(m_msg))
        sys.exit(-1)

    source = socket.gethostname()
    results = m_rds.hgetall('gain_stat_hash')
    with mdb.cursor() as m_cursor:
        for field in results:
            items = field.split('|')
            if len(items) < 4:
                continue
            day = items[0]
            hour = items[1]
            queue = items[2]
            q_type = items[3]
            number = int(results[field])
            sql = '''insert into scrapy_stat(`day`, `hour`, `task`, `type`, `num`, `source`)
                    values ('{0}', '{1}', '{2}', '{3}', {4}, '{5}')
                    '''.format(day, hour, queue, q_type, number, source)
            execute_sql(m_cursor, sql)
    mdb.commit()
