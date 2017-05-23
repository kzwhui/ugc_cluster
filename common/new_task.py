#!/usr/bin/python
#encoding=utf8
import re
import pickle
import sys
import time,datetime
import traceback
import logging
import collections

sys.path.append('../common')
from db import DBQueryPool
from log import logger
reload(sys) 
sys.setdefaultencoding('utf-8')

def set_failed_task(db_key, table, id, max_id, msg):
    if not table:
        logger.error("invalid params? table=%s, id=%s, max_id=%s, msg=%s" % (table, id, max_id, msg))
        return 0
    logger.debug("table=%s, id=%s, max_id=%s, msg=%s" % (table, id, max_id, msg))
    sql = "update %s set c_status = 'fail', c_handle_result='%s' where c_id <= %s and c_status = 'new' and c_entity_id = '%s'"\
                % (table, msg, max_id, id)
    afects = DBQueryPool.get_instance(db_key).execute_sql(sql)
    return afects 


class Task(object):
    def __init__(self, table_name, db_key):
        self.table_name = table_name
        self.db_key = db_key
        #self.max_id_dict = {}

    def get_task_id(self, status, max_count, filter_condition=None):
        sql = "select max(c_id) as incr_id, c_entity_id as entity_id from %s where c_status = '%s' and (%s) group by c_entity_id" \
                % (self.table_name, status, filter_condition )
        if not filter_condition:
            sql = "select max(c_id) as incr_id, c_entity_id as entity_id from %s where c_status = '%s' group by c_entity_id" \
                    % (self.table_name, status)

        if max_count == 0:
            return {}
        elif max_count:
            sql += " limit %s" % max_count

        rows = DBQueryPool.get_instance(self.db_key).get_dict(sql)
        #self.max_id_dict = dict(map(lambda r: (r['entity_id'], r['incr_id']), rows))
        #return self.max_id_dict.keys()
        self.max_id_dict = dict(map(lambda r: (r['entity_id'], r['incr_id']), rows))
        return self.max_id_dict

    def get_task_id_by_entity_type(self, status, entity_type, max_count, filter_condition=None):
        new_condition = '1'
        if entity_type:
            new_condition = "c_entity_type = '%s'" % entity_type
        if filter_condition:
            new_condition = "(%s) and (%s)" % (new_condition, filter_condition)
        return self.get_task_id(status, max_count, new_condition)
    
    #xugc is very special,include add and delete data and need to keep sequence
    def get_task_id_for_xugc(self, status, max_count, filter_condition=None):
        new_condition = '1'
        if filter_condition:
            new_condition = "(%s) and (%s)" % (new_condition, filter_condition)

        if new_condition:
            sql = "select t.c_id,t.c_entity_id,t.c_task_type,t.c_create_time from \
                (select c_id, c_entity_id, c_task_type,c_create_time from %s where c_status = '%s' and (%s) order by c_create_time desc) as t group by t.c_entity_id" \
                % (self.table_name, status, new_condition )
        else:
            sql = "select t.c_id,t.c_entity_id,t.c_task_type,t.c_create_time from \
                (select c_id, c_entity_id, c_task_type,c_create_time from %s where c_status = '%s' order by c_create_time desc) as t group by t.c_entity_id" \
                    % (self.table_name, status)

        if max_count == 0:
            return {}
        elif max_count:
            sql += " limit %s" % max_count

        rows = DBQueryPool.get_instance(self.db_key).get_dict(sql)
        add_rows = filter(lambda x:x['c_task_type']=='push_all', rows)
        delete_rows = filter(lambda x:x['c_task_type']!='push_all', rows)

        self.max_id_dict = dict(map(lambda r: (r['c_entity_id'], r['c_id']), add_rows))
        self.max_id_dict_del = dict(map(lambda r: (r['c_entity_id'], r['c_id']), delete_rows))
        return self.max_id_dict, self.max_id_dict_del

    def get_task_id_by_entity_type_and_create_time(self, status, entity_type, create_time_interval_second=86400,
            max_count=200, filter_condition=None):
        new_condition = "c_create_time > date_sub(now(), interval %s second)" % create_time_interval_second
        if filter_condition:
            new_condition = "(%s) and (%s)" % (new_condition, filter_condition)
        return self.get_task_id_by_entity_type(status, entity_type, max_count, new_condition)

    def set_task_status(self, max_id_dict, id_list, status, assign_part=None, filter_condition=None):
        if not id_list:
            logger.error("id_list empty, status=%s" % status)
            return

        max_incr_id = max(map(lambda id: max_id_dict.get(id), id_list))
        valid_id_list = filter(lambda id: max_id_dict.get(unicode("%s" % id)), id_list)
        if not valid_id_list:
            logger.error("valid_id_list is empty, status=%s, id_list=[%s] " % (status, id_list))
            #logger.error("max_id_dict=%s, id_list=%s" % (max_id_dict, id_list))
            return

        part_assign = ''
        if assign_part: 
            part_assign = ", %s" % assign_part

        sql = "update %s set c_status = '%s'%s where (c_status in ('new', 'fail') and c_id <= %s and c_entity_id in (%s))" \
                % (self.table_name, status, part_assign, max_incr_id, ', '.join(map(lambda a: "'%s'" % a, valid_id_list)))
        if filter_condition:
            sql += " and (%s)" % filter_condition
        DBQueryPool.get_instance(self.db_key).execute_sql(sql)

    def insert_new_task(self, id_list, entity_type, operator = '', extra_fields_dict = {}):
        if not id_list or not entity_type:
            logger.error("invalid params:%s, %s" % (id_list, entity_type))
            return -1
        keys_str = ''
        value_str = ''
        if extra_fields_dict:
            for k, v in extra_fields_dict.items():
                keys_str += ','
                keys_str += "%s" % k
                value_str += ','
                value_str += " '%s'" % v

        sql = "insert into %s (c_entity_id, c_entity_type, c_status, c_operator, c_create_time %s) values %s" % \
                (self.table_name, keys_str, \
                 ',' . join(["('%s', '%s', 'new', '%s', now() %s)" % (id, entity_type, operator, value_str) for id in id_list]))
        afects = DBQueryPool.get_instance(self.db_key).execute_sql(sql)
        return afects 

    def get_new_and_fail_task(self, entity_type, fail_task_create_time_interval_second, max_count):
        entity_id_to_incr_id_dict = self.get_task_id_by_entity_type('new', entity_type, max_count)
        fail_task_max_count = max_count
        if entity_id_to_incr_id_dict:
            fail_task_max_count = max_count - len(entity_id_to_incr_id_dict)
        entity_id_to_incr_id_dict.update(
                self.get_task_id_by_entity_type_and_create_time('fail', entity_type, fail_task_create_time_interval_second, fail_task_max_count)
                )
        return entity_id_to_incr_id_dict

    def rewrite_back_status(self, entity_id_to_incr_id_dict, result_dict):
        for status, type_dict in result_dict.items():
            for result_type, id_list in type_dict.items():
                if id_list:
                    logger.debug("%s.%s=%s" % (status, result_type, id_list))
                    self.set_task_status(entity_id_to_incr_id_dict, id_list, status, assign_part="c_handle_result = '%s'" % result_type)

    @staticmethod
    def print_result_dict(result_dict):
        for status, type_dict in result_dict.items():
            for result_type, id_list in type_dict.items():
                if id_list:
                    print "%s.%s=%s" % (status, result_type, id_list)

if __name__ == '__main__':
    task = Task('t_play_source_incr_task', db_key='task')
    print task.get_task_id_by_entity_type('new', 'cover', 3, "c_task_type = 'outer'")
    task.set_task_status(['q2pe89idtm3m5eo'], 'fail', "c_comment = 'test'", "c_task_type = 'outer'")
