#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright 2014 xiongli
#
# @mail: xionglihdfs@163.com
# @home: 
# @QQ:   2710887018 
# @date: Sep 14, 2014
#

import sys
import time
import types
import psycopg2
import psycopg2.extras

reload(sys)
sys.setdefaultencoding('utf-8')

class find_indexes:
    """
    """
    def __init__(self, indexes):
        self.indexes = indexes

class rebuild_indexes:
    """
    """
    def __init__(self, indexes):
        self.indexes = indexes

if __name__ == '__main__':
    hostip = '127.0.0.1'
    db_password = 'xxxxxxxxxx'

    sql = "set search_path = radius ; select indexname from pg_indexes where schemaname='radius'"
    conn = psycopg2.connect(host=hostip, port=5432, user='postgres', password=db_password, database='thebitsea')
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    rebuild_indexes = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.execute(sql)
    indexes_num = cursor.rownumber
    print "重建索引的个数："
    print indexes_num

    sum = 0

    for indexname in cursor.fetchall():
        print "开始重建："
        print indexname
        sum = sum + 1
        rebuild_indexes.execute("ANALYZE %s" % indexname[0])
        rebuild_indexes.execute('REINDEX INDEX %s' % indexname[0])
        rebuild_indexes.execute('ANALYZE %s' % indexname[0])
        print sum

    print sum
    print "ok"


        
