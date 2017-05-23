#!/usr/bin/python
#encoding=utf8

'''
访问数据库的接口
简单易用型
'''

import sys
import time
import MySQLdb
import traceback
from log import logger
sys.path.append('../conf')
from conf import g_conf

class DBWrapper(object):
    def __init__(self, db_config):
        self.__db_config = db_config.copy()
        self.__conn = None

    def __connect(self):
        if self.__conn:
            self.__conn.close()

        self.__conn = None
        for i in xrange(10):
            try:
                self.__conn = MySQLdb.connect(**self.__db_config)
                logger.info('suc connect to mysql, config=%s' % self.__db_config)
                break
            except:
                logger.error(traceback.format_exc())
                logger.error("fail to connect mysql, config=%s" % self.__db_config)
            time.sleep(1)

        return True if self.__conn else False

    def __is_valid_conn(self):
        if not self.__conn:
            return False

        try:
            self.__conn.ping()
            return True
        except:
            logger.error('mysql connect has gone away')

        return False

    # 插入、删除、更新操作，返回affect，表示影响多少行，或是否成功
    def execute_sql(self, sql):
        if not self.__is_valid_conn() and not self.__connect():
            return False

        cursor = self.__conn.cursor()
        affect = None
        try:
            logger.debug('execute sql=%s' % sql)
            affect = cursor.execute(sql)
            self.__conn.commit()
        except:
            logger.error(traceback.format_exc())
            logger.error('execute sql=%s' % sql)
            self.__conn.rollback()
            raise Exception("fail execute sql, sql=%s" % (sql))
        cursor.close()

        return affect

    # 查询操作，返回获取的记录列表
    def get_dict(self, sql):
        if not self.__is_valid_conn() and not self.__connect():
            return None

        cursor = self.__conn.cursor(MySQLdb.cursors.DictCursor)
        results = None
        try:
            logger.debug('execute sql=%s' % sql)
            cursor.execute(sql)
            self.__conn.commit()
            results = cursor.fetchall()
        except:
            logger.error(traceback.format_exc())
            logger.error('execute sql=%s' % sql)
            self.__conn.rollback()
            raise Exception("fail execute sql, sql=%s" % (sql))
        cursor.close()

        return results

class DBWrapperFactory:
    @staticmethod
    def get_instance(db_key):
        db_config = g_conf.DB_CONF[db_key]
        return DBWrapper(db_config)

########################################################################
def do_test():
    db_wrapper = DBWrapperFactory.get_instance('d_crawler_info')
    affect = db_wrapper.execute_sql("insert into t_test (c_book_title) values('test')")
    rows = db_wrapper.get_dict('select * from t_test where c_book_title = "test" order by c_id desc')
    print 'affect=%s' % affect
    print 'rows=%s' % rows[0]

if __name__ == '__main__':
    do_test()
