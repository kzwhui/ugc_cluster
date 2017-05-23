#!/usr/bin/python
#encoding=gbk
###############
#�������ݿ�ӿ�
#   �ṩ����:�������ݿ�ִ��sql��䣻ѡȡ���ݿ��¼����tuple��dict���������ʽ����
#
#
#   author: okicui
#   date  : 2012-11-22
###############

import re
import pickle
import sys
import MySQLdb
import pdb
import traceback
import time,datetime
from MySQLdb import escape_sequence
from MySQLdb.cursors import DictCursor
import _mysql_exceptions as MyExceptions
import logging
from log import logger
sys.path.append('../conf')
from config import g_conf
from boss_api import *

def get_caller(orgmode=False):
    """���ط����ĵ�����,�����־ʱ��Ҫ,�������"""
    import inspect
    stack=inspect.stack()[2]
    if orgmode:
        return "[[file:"+stack[1][stack[1].rfind('/')+1:]+"::"+str(stack[2])+"]] "+stack[3]
    else:
        return stack[1][stack[1].rfind('/')+1:]+":"+str(stack[2])+" "+stack[3]

class DBQueryBase(object):
    """���ݿ��ѯ����
       �������ݿ��װ��,�ṩ���ַ������ݿ�ķ���
    """
    #vv @staticmethod
    def get_conn(self, cursorcls=None):
        #if cursorcls in self.CURSOR_DICT and self.CURSOR_DICT[cursorcls]:
        #    return self.CURSOR_DICT[cursorcls]
        """��ȡ���ݿ�����
        cursorcls Ϊ MySQLdb.cursors.DictCursorʱ,��ѯ��������ֵ����ʽ����,���ݿ�������Ϊkey�����ݿ��¼��ֵΪvalue
        ����ʱ unix_socket��Ҫ��Ϊ/tmp/mysql.sock
        """
        mcdbDict = g_conf.DB_CONF[self.db_key].copy()
        #mcdbDict = {
        #    'host'         : g_conf.DB_HOST,
        #    'user'         : g_conf.DB_USER,
        #    'passwd'       : g_conf.DB_PASSWORD,
        #    'port'         : g_conf.DB_PORT,
        #    'db'           : g_conf.DB_DATABASE,
        #    'charset'      : g_conf.DB_CHARSET,
        #    'unix_socket'  : '/tmp/mysql.sock',
        #    'init_command' : "SET NAMES '%s';" % g_conf.DB_CHARSET
        #    }
        mcdbDict['unix_socket']  = '/tmp/mysql.sock'
        mcdbDict['init_command'] = "SET NAMES '%s'" % mcdbDict['charset']

        # never use backup
        mcdbBakDict = mcdbDict
        #mcdbBakDict = {
        #    'host'         : g_conf.DB_HOST_BAK,
        #    'user'         : g_conf.DB_USER_BAK,
        #    'passwd'       : g_conf.DB_PASSWORD_BAK,
        #    'port'         : g_conf.DB_PORT_BAK,
        #    'charset'      : g_conf.DB_CHARSET,
        #    'db'           : g_conf.DB_DATABASE_BAK,
        #    'unix_socket'  : '/tmp/mysql.sock',
        #    'init_command' : "SET NAMES '%s';"   % g_conf.DB_CHARSET
        #    }

        if cursorcls:
            mcdbBakDict['cursorclass']=cursorcls
            mcdbDict['cursorclass']=cursorcls

        conn = None
        for loop in range(0,10):
            try:
                logger.debug("Before MySQLdb.connect, config=%s" % (mcdbDict))
                conn = MySQLdb.connect(**mcdbDict)
                logger.debug("After MySQLdb.connect")
                break
            except:
                attr_add(454322, 1)
                logger.error('[�������ݿ�(����)ʧ��]:'+str(mcdbDict))
                logger.error(traceback.format_exc())
            time.sleep(1)

        if not conn:
            for loop in range(0,10):
                try:
                    logger.debug("Before MySQLdb.connect bak")
                    conn = MySQLdb.connect(**mcdbBakDict)
                    logger.debug("After MySQLdb.connect bak")
                    break
                except:
                    attr_add(454322, 1)
                    logger.error('[�������ݿ�(����)ʧ��]:'+str(mcdbDict))
                    logger.error(traceback.format_exc())
                time.sleep(1)
        if conn:
            self.CURSOR_DICT[cursorcls] = conn
            return conn
        ##�������ݿⶼ��������ʱ����
        alarm("[����]�޷������κ����ݿ�",dead=True , alarm_level=g_conf.ALARM_LEVEL_SERVER_CONDB_FAIL)


    #vv @staticmethod
    def execute_sql(self, sql):
        """ִ��һ��sql���
        ������Ӱ��ļ�¼����ִ��ʧ�ܻ����쳣����-1
        """

        conn=self.get_conn()
        cur=conn.cursor()
        affect = -1
        try:
            logger.debug('[DB] '+get_caller()+' \nsql='+sql)
            affect = cur.execute(sql)
            logger.debug('[DB] affect=%d'%(affect))
            conn.commit()
        except:
            logger.error(traceback.format_exc())
            # ���ӱ���
            conn.rollback()
            raise Exception("execute_sql fail, sql=%s" % (sql))
        cur.close()
        #conn.close()
        return affect

    #vv @staticmethod
    def executemany(self, sql, values):
        conn = self.get_conn()
        cur = conn.cursor()
        affect = 0
        try:
            step = 10000
            logger.debug('[DB] sql=%s, step=%s'%(sql, step))
            for i in range((len(values) - 1)/step + 1):
                start = i * step
                end = (i + 1) * step
                #logger.debug( "[DB] "+get_caller()+" \nsql="+ sql % conn.literal(values[start:end]))
                affect += cur.executemany(sql, values[start:end])
                logger.debug('[DB] affect=%d'%(affect))
            conn.commit()
        except:
            logger.error(traceback.format_exc())
            conn.rollback()
            cur.close()
            raise Exception("db error, sql=%s" % (sql))
        cur.close()
        return affect

    ###vv @staticmethod
    ##def insert_object(self, cls, object, columns=[], table_name=None):
    ##    objects = [object]
    ##    return self.insert_objects(cls, objects, columns, table_name)

    ###vv @staticmethod
    ##def insert_objects(self, cls, objects, columns=[], table_name=None):
    ##    values = []
    ##    if not columns:
    ##        columns = cls.KEYS
    ##    for  obj in objects:
    ##        values.append(tuple([unicode(obj.__dict__[k]) for k in columns]))

    ##    t = table_name
    ##    if not t :
    ##        t = cls.DB_TABLE
    ##    sql = 'INSERT INTO %s(%s) VALUES(%s)' % (t \
    ##            ','.join(map(lambda s: '`%s`' % s, columns)), \
    ##            ','.join(['%s'] * len(columns)))
    ##    return self.executemany(sql, values)

    #vv @staticmethod
    def execute_batch(self, sqls):
        """ִ��һ��sql��䣬�����Ƿ�ı����ݿ��¼��"""
        changed=False
        conn=self.get_conn()
        cur=conn.cursor()
        try:
            for sql in sqls:
                logger.debug('[DB] '+get_caller()+' \nsql='+sql)
                affect = cur.execute(sql)
                logger.debug('[DB] affect=%d'%(affect))
            conn.commit()
            changed=True
        except:
            logger.error(traceback.format_exc())
            conn.rollback()
            raise Exception("execute_sql fail, sql=%s" % (sqls))
        cur.close()
        #conn.close()
        return changed

    #vv @staticmethod
    def get_dict(self, sql,params=[]):
        """#ͨ��sql��ȡ���ݿ��¼���ֵ�
        params Ϊ sql�� %s��Ӧ�Ĳ�������
        #�����ֵ�����,keyΪ select ����,valueΪ���Ӧֵ

        ��, get_dict('select * from client where id=%s and service=%s',[12,'cb'])

        """
        conn=self.get_conn(MySQLdb.cursors.DictCursor)
        ##logger.debug( "[DB] sql="+sql)
        if params:
            logger.debug( "[DB] "+get_caller()+" \nsql="+ sql % conn.literal(params))
        else:
            logger.debug( "[DB] "+get_caller()+" \nsql="+ sql)
        try:
            cur = conn.cursor()
            ##ִ�в�ѯ
            if params:
                cur.execute(sql,params)
            else:
                cur.execute(sql)
            # ��ֹ֮ǰ�ĸ���δ�ύ���ٴ��ύ
            conn.commit()
            ##��ȡ���н��
            alldict=cur.fetchall()
            cur.close()
        except Exception,e:
            logger.error(traceback.format_exc())
            raise Exception("db error, sql=%s" % (sql))
            alldict={}
        return alldict

    #vv @staticmethod
    def get_tuple(self, sql,params=[]):
        """ͨ��sql��ȡ���ݿ���tuple
        ��, get_dict('select * from client where id=%s and service=%s',[12,'cb'])
        """
        conn=self.get_conn()
        logger.debug( "[DB] "+get_caller()+" \nsql="+ sql % conn.literal(params))
        cur=conn.cursor()
        cur.execute(sql,params)
        alltuple=cur.fetchall()
        cur.close()
        #conn.close()
        return alltuple

    #vv @staticmethod
    def get_table_dict(self, table,params=[],columns=[],**cond):
        """ͨ��������ȡ��Ӧ���ֵ�
        columns Ϊ����(����),�� ['id agentID',]
        """
        if not columns:
            sql = "SELECT * FROM "+table+" "
        else:
            #���������escape����
            cls=[]
            for s in columns:
                s=s.strip()
                if s.count(' ')==1:
                    cls.append("`"+s.replace(' ','` '))
                elif s.count(' ')==0:
                    cls.append("`"+s+"`")
                else:
                    raise Exception("too much space in column "+s)
            sql = "SELECT "+ ",".join(cls) + " FROM "+table

        ps=params[:]
        if cond:
            sql+=" WHERE "+" AND ".join(["`%s` = %%s" %(k) for k in cond.keys()])

        for k in cond.keys():
            ps.append(cond[k])

        return self.get_dict(sql,ps)

    #vv @staticmethod
    def gen_insert_sql(self, cls, columns=[], values=[]):
        '''
        '''
        escape_values = []
        for v in values:
            escape_values.append("%s" % MySQLdb.escape_string("%s" % v))
        conn=self.get_conn()
        sql = 'INSERT INTO %s(%s) VALUES(%s)' % (cls.DB_TABLE, \
                ','.join(map(lambda s: '`%s`' % s, columns)), \
                ','.join(map(lambda s: '%s' % s, conn.literal(escape_values))))
        return sql

    #vv @staticmethod
    def gen_delete_sql(self, cls,**cond):
        conn=self.get_conn()
        sql = 'DELETE FROM %s' % cls.DB_TABLE
        assert cond
        if cond:
            sql += " WHERE "+" AND ".join(["`%s`=%%s" %(k) for k in cond.keys()])
        params = []
        for k in cond.keys():
            params.append(cond[k])
        sql = sql % conn.literal(params)
        return sql

    #vv @staticmethod
    def gen_insert_update_sql(self, table_name, update_dict, duplicated_dict, id_field_name='', id_value = ''):
        conn=self.get_conn()
        if not table_name or not update_dict or not id_field_name or not id_value or not duplicated_dict:
            logger.error("invalid params?")
            return ''
        if not isinstance(duplicated_dict, dict):
            logger.error("invalid params, what type of:%s, %s", (type(duplicated_dict), duplicated_dict))
            return ''
            
        sql = "insert into %s (%s, %s) values ('%s', %s) ON DUPLICATE KEY UPDATE %s" % \
                (table_name, id_field_name, ',' . join(['%s' % f for f in update_dict.keys()]),\
                 id_value, ',' . join(["'%s'" % MySQLdb.escape_string("%s" % v) for k, v in update_dict.items()]), \
                 ",".join(["%s='%s'" % (k, MySQLdb.escape_string("%s" % v)) for k, v in duplicated_dict.items()]))
        return sql

    #vv @staticmethod
    def gen_update_sql(self, table_name,columns=[],values=[],**cond):
        """Ϊ���¶������������sql���
        ��,self.gen_update_sql(Client,['service_or_task'],[DBC.STATUS_TASK],id=123)
        �����ظ���client��service_or_task�У�ֵΪTASK,����Ϊid=123
        ��: update client set service_or_task=TASK wher id=123
        """
        conn=self.get_conn()
        if not columns or not values or len(columns)!=len(values):
            return '-1'
        params = values[:]
        sql = "UPDATE "+ table_name+" SET " + ",".join(map(lambda s : "`"+s+"`=%s ",columns))
        if cond:
            sql+=" WHERE "+" AND ".join(["`%s`=%%s" %(k) for k in cond.keys()])

        for k in cond.keys():
            params.append(cond[k])
        sql = sql % conn.literal(params)
        #conn.close()
        return sql

    #vv @staticmethod
    def update_object(self, cls,columns=[],values=[],**cond):
        """����cls��Ӧ�ı�
        ��,self.update_object(Taskdef , ['status','end_time'],[DBC.STATUS_ERR,nowTime()],id=self.id)
        ��taskdef���е�status,end_time����ΪERR,����ʱ�䣬����ʱid=1
        """
        conn=self.get_conn()

        if not columns or not values or len(columns)!=len(values):
            return -1
        params=values[:]

        sql = "UPDATE "+cls.DB_TABLE+" SET " + ",".join(map(lambda s : "`"+s+"`=%s ",columns))

        if cond:
            sql+=" WHERE "+" AND ".join(["`%s` = %%s" %(k) for k in cond.keys()])

        for k in cond.keys():
            params.append(cond[k])

        logger.debug( "[DB] "+get_caller()+" \nsql="+ sql % conn.literal(params) )
        #pdb.set_trace()
        try:
            cur=conn.cursor()
            affect=cur.execute(sql,params)
            conn.commit()
        except:
            logger.error(traceback.format_exc())
            print traceback.format_exc()
            conn.rollback()
        cur.close()
        #conn.close()
        return affect

    #vv @staticmethod
    def load_objects_by_sql(self, cls,sql):
        """��������condѡȡ����,�����ݿ�õ�ÿ�м��뵽�����������
        �����������������Է��ʣ��� agent.id ���������ݿ�id�ж�Ӧ��ֵ
        ���columnsΪNone����ѡ��������
        coloumns����Ϊ('col alias','col2 alias2')...
        """
        dbObs=self.get_dict(sql)
        objs=[]
        for o in dbObs:
            obj=cls()
            for k,v in o.items():
                obj.__setattr__(k,v)
            objs.append(obj)
        return objs

    #vv @staticmethod
    def load_objects(self, cls,columns=[],**cond):
        """��������condѡȡ����,�����ݿ�õ�ÿ�м��뵽�����������
        �����������������Է��ʣ��� agent.id ���������ݿ�id�ж�Ӧ��ֵ
        ���columnsΪNone����ѡ��������
        coloumns����Ϊ('col alias','col2 alias2')...
        """
        #logger.debug("columns="+str(columns))
        #logger.debug("cond="+ str( cond))
        if not columns:
            sql = "SELECT * FROM  %s" % cls.DB_TABLE
        else:
            sql = "SELECT %s FROM  %s" %  (",".join(columns),cls.DB_TABLE)

        # pricesong: ����Ϊʲô��ֱ�Ӱ�paramsҲд��sql��
        params=[]
        if cond:
            sql+=" WHERE "+" AND ".join(["`%s` = %%s" %(k) for k in cond.keys()])

        for k in cond.keys():
            params.append(cond[k])
        #sql += ' limit 10'
        dbObs=self.get_dict(sql,params)
        objs=[]
        for o in dbObs:
            obj=cls()
            for k,v in o.items():
                obj.__setattr__(k,v)
            objs.append(obj)
        return objs

    #vv @staticmethod
    def load_object_by_id(self, cls,id,columns=[]):
        """ͨ��������ȡ����"""
        cond={str(cls.PRIMARY_KEY):id}
        objs = self.load_objects(cls,columns,**cond)
        if len(objs)==0:
            #raise Exception("no record in table "+cls.DB_TABLE+" "+cls.PRIMARY_KEY+"="+pkid)
            return None
        elif len(objs)>1:
            raise Exception(str(len(objs))+" records in table "+cls.DB_TABLE+" "+cls.PRIMARY_KEY+"="+id)
        else:
            return objs[0]

class DBQueryObject(DBQueryBase):
    def __init__(self, db_key):
        self.CURSOR_DICT = {}
        self.db_key = db_key

class DBQueryPool:
    KEY_TO_CONN_DICT = {}

    @staticmethod
    def get_instance(db_key):
        if db_key not in DBQueryPool.KEY_TO_CONN_DICT:
            DBQueryPool.KEY_TO_CONN_DICT[db_key] = DBQueryObject(db_key)
        return DBQueryPool.KEY_TO_CONN_DICT[db_key]

DBQuery = DBQueryPool.get_instance("sdp")
