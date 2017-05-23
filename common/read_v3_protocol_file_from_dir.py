#!/usr/local/bin/python
#encoding=utf8

import sys
import getopt
import logging
import traceback
import os
import time
import re
import json
from datetime import datetime
import MySQLdb

from pprint import pprint
from log import logger
from db import DBQueryPool
from common import rs_hash, get_table_number_dict, get_table_number_dict_for_obj
from new_task import Task
from config import g_conf
from redis_helper import CRedisHelper

reload(sys) 
sys.setdefaultencoding('utf-8')
debug_flag = 0
def print_debug_info(obj_list, attribute_name_list = None, extra_str = None, dump_all = False):
    one_o = None
    if obj_list:
        one_o = obj_list[0]
    if not extra_str:
        extra_str = "print_debug_info"
    print "/////////%s, len=%d, class=%s////////////////" % (extra_str , len(obj_list), one_o)
    cnt = 1
    for o in obj_list:
        cnt = cnt - 1
        if attribute_name_list: 
            print "%s, id=%s, \n" % (' | ' . join(['%s:%s' % (attr, o.__getattribute__(attr)) for attr in attribute_name_list]), o.get_id())
            continue
        else:
            print "########################";
            print "objs =%s\n%s\n" % (o, '\n' . join(['%s:%s' % (k, v) for k, v in o.__dict__.items()]))
            print "************************\n";
        if cnt < 0 and not dump_all:
            break
    print "########################";

def report_info(c_file_name, c_file_type_id, c_deal_type):
    if c_deal_type not in set(["create", "push", "effect", "handle", "check"]):
        logger.error("invalid report params:[%s], [%s], [%s]"  %(c_file_name, c_file_type_id, c_deal_type))
    sql = "insert into t_file_deal_step (c_file_name,c_file_type_id,c_deal_type,c_start_time,c_end_time,c_operator,c_create_time) values('%s', '%s', '%s', now(), now(), '%s', now())"\
            % (c_file_name, c_file_type_id, c_deal_type, sys.argv[0])
    afects = DBQueryPool.get_instance("Search").execute_sql(sql)
    return afects 

class CMergeTwoDirectorIterator(object):
    """
    因为new和mv文件存储在不同的文件夹，而且数据这2个目录的文件更新可能存在差异，则需要
    记录2个目录的文件读取时间，然后每次都取2个目录的较小的时间磋来过滤2个目录的文件，这样记录文件就需要记录2个文件的时间了
    """
    def __init__(self, new_file_dir,mv_file_dir, store_file):
        self.new_file_directory = new_file_dir
        self.mv_file_directory = mv_file_dir
        self.last_file_timestamp = None                      #上次读取的文件时间戳（文件名就是时间戳）
        install_path = g_conf.INSTALL_PATH
        self.store_last_mv_file = '%s/var/mv_%s' % (install_path , store_file )#没读取完一个文件会把时间戳存储到这里
        self.store_last_new_file = '%s/var/new_%s' % (install_path , store_file) #没读取完一个文件会把时间戳存储到这里
        self.is_read_end = False
        self.file_array = []
        self.index = 0

    def _iterator_one_directory(self, dir, last_timestamp, expression):
        pathDir = os.listdir(dir)
        pattern = re.compile(expression)
        sorted_timestamp = []
        map_timestamp_to_file_name = {}
        for file in pathDir:
            group_match = pattern.findall(file)
            child_timestamp = None
            if group_match:
                if 1 == debug_flag:
                    logger.debug("file=%s, group_match=%s" % (file, group_match))
                child_timestamp = group_match[0]
                sorted_timestamp.append(long(child_timestamp))
                if map_timestamp_to_file_name.get(long(child_timestamp), None):
                    map_timestamp_to_file_name[long(child_timestamp)].add(file)
                else:
                    map_timestamp_to_file_name[long(child_timestamp)] = set([file])
            else:
                print "not match rule[%s],[%s]" % (expression, file)
        sorted_timestamp.sort()
        return_time_stamp=[]
        print ("sorted_timestamp=%s" % sorted_timestamp)
        logger.debug("sorted_timestamp=%s" % sorted_timestamp)
        if last_timestamp:
            for t in sorted_timestamp:
                if t <= last_timestamp:
                    logger.debug("ignore this timestamp=%s, last=%s" % (t, last_timestamp))
                    print("ignore this timestamp=%s, last=%s" % (t, last_timestamp))
                    map_timestamp_to_file_name.pop(t)
                    continue
                return_time_stamp.append(t)
        else:
            return_time_stamp = sorted_timestamp

        return return_time_stamp , map_timestamp_to_file_name

    def _merge_2_files(self, sorted_1, sorted_2, map1, map2):
        sorted_all = sorted_1
        for t in sorted_2:
            sorted_all.append(t) 

        if not sorted_all:
            return []
        sorted_all.sort()
        print ("sorted_all=%s" % sorted_all)
        logger.debug("sorted_all=%s" % sorted_all)
        file_list = []
        for t in sorted_all: 
            f_set = map1.get(t, None)
            if f_set:
                for f in f_set:
                    file_list.append(f) 
            f_set = map2.get(t, None)
            if f_set:
                for f in f_set:
                    file_list.append(f) 

        return file_list

    def get_one_new_file_to_read(self):
        type = ""
        if self.is_read_end:
            logger.error("not to call me again")
            return None, type 

        if not self.new_file_directory or not os.path.exists(self.new_file_directory) or \
           not self.mv_file_directory or not os.path.exists(self.mv_file_directory):
            logger.error("no such dir:[%s] [%s]" % (self.new_file_directory, self.mv_file_directory))
            self.is_read_end = True
            return None, type 

        if self.file_array:
            if len(self.file_array) <= self.index:
                self.is_read_end = True
                return None, type 
            file = self.file_array[self.index]
            self.index = self.index + 1
            type = self.get_suffix_type(file)
            return file, type 

        if not self.last_file_timestamp: 
            self.last_file_timestamp = self.get_last_file_name(self.store_last_mv_file) 
            x = self.get_last_file_name(self.store_last_new_file) 
            logger.info("mv last=%s, new last=%s" % (self.last_file_timestamp , x))
            print("mv last=%s, new last=%s" % (self.last_file_timestamp , x))
            if self.last_file_timestamp > x:
                self.last_file_timestamp = x

        pattern_mv = r'(\d+)\.(?:mv)$' #注意正则，不同文件名格式不一样
        pattern_new = r'(\d+)\.(?:new)$' #注意正则，不同文件名格式不一样
        sorted_timestamp_mv = []
        sorted_timestamp_new = []
        map_timestamp_to_file_name_mv = {}
        map_timestamp_to_file_name_new = {}

        sorted_timestamp_mv, map_timestamp_to_file_name_mv = self._iterator_one_directory(self.mv_file_directory, self.last_file_timestamp, pattern_mv)
        sorted_timestamp_new, map_timestamp_to_file_name_new = self._iterator_one_directory(self.new_file_directory, self.last_file_timestamp, pattern_new)
        self.file_array = self._merge_2_files(sorted_timestamp_mv, sorted_timestamp_new, map_timestamp_to_file_name_mv, map_timestamp_to_file_name_new)
        print "file_array = %s" % self.file_array 
        logger.info("file_array = %s" % self.file_array) 
        if not self.file_array:
            self.is_read_end = True
            logger.error("no file exists:[%s] [%s]" % (self.new_file_directory, self.mv_file_directory))
            return None, type 

        file = self.file_array[self.index]
        self.index = self.index + 1
        type = self.get_suffix_type(file)
        return file, type 
    
    def get_suffix_type(self, name):
        if not name:
            return ''
        pattern = re.compile(r'\.(mv|new|new_part_\d+)$') #注意正则，不同文件名格式不一样
        group_match = pattern.findall(name)
        logger.debug("file=%s, group_match=%s" % (file, group_match))
        if group_match and isinstance(group_match, list):
            return group_match[0]
        return group_match 

    def get_last_file_name(self, store_last_file):
        if (os.path.exists(store_last_file)):
            if (os.path.getsize(store_last_file) > 0):
                time_file = open(store_last_file, "r")
                cur_last_file_name = time_file.read()
                time_file.close()
                if cur_last_file_name:
                    return long(cur_last_file_name)
            else:
                logger.debug("empty file:%s" % store_last_file)
        else:
            logger.debug("no such file:%s" % store_last_file)
        return 0

    def record_last_file_name(self, file_name):
        if not file_name:
            logger.error("no rule emtpy match") 
            return None
        pattern = re.compile(r'(\d+)\.(mv|new|new_part_\d+)$') #注意正则，不同文件名格式不一样
        group_match = pattern.findall(file_name)
        print "group_match = %s" % group_match 
        if not group_match:
            logger.error("no rule match??[%s]" % file_name)
            return None
        t = group_match[0]
        if not isinstance(t, tuple):
            logger.error("sth wrong? file_name=[%s] timestamp=[%s], type=%s" % (file_name, t, type(t)))
            return
        last_time = t[0]
        type = t[1]
        logger.info("file_name=[%s] timestamp=[%s], last_time=%s, type=%s" % (file_name, t, last_time, type))
        store_last_file = self.store_last_mv_file 
        if 'new' == type: 
            store_last_file = self.store_last_new_file 

        time_file = open(store_last_file, "w")
        time_file.write("%s" % last_time)
        time_file.close()


class CDiretoryIterator(object):
    def __init__(self, dir, store_file, sync_all, remain_num = 0):
        self.remain_num = remain_num 
        self.force_sync = sync_all
        self.directory = dir
        install_path = g_conf.INSTALL_PATH
        self.store_last_new_file = '%s/var/new_%s' % (install_path, store_file)#没读取完一个文件会把时间戳存储到这里
        self.is_read_end = False
        self.file_array = []
        self.index = 0
        self.last_file_name = 0
        self.store_last_file = "%s/var/%s" % (install_path, store_file)

    def get_one_new_file_to_read(self):
        type = ""
        if self.is_read_end:
            logger.error("not to call me again")
            return None, type 

        if not self.directory or not os.path.exists(self.directory): 
            logger.error("no such dir:[%s]" % self.directory)
            self.is_read_end = True
            return None, type 
        if self.file_array:
            if len(self.file_array) <= self.index:
                self.is_read_end = True
                return None, type 
            file = self.file_array[self.index]
            self.index = self.index + 1
            type = self.get_suffix_type(file)
            return file, type 

        if not self.last_file_name: 
            self.get_last_file_name() 
        pathDir = os.listdir(self.directory)
        pattern = re.compile(r'(\d+)\.(?:mv|new|new_part_\d+)$') #注意正则，不同文件名格式不一样
        sorted_timestamp = []
        map_timestamp_to_file_name = {}
        for file in pathDir:
            group_match = pattern.findall(file)
            child_timestamp = None
            if group_match:
                if 1 == debug_flag:
                    logger.debug("file=%s, group_match=%s" % (file, group_match))
                child_timestamp = group_match[0]
                sorted_timestamp.append(long(child_timestamp))
                if map_timestamp_to_file_name.get(long(child_timestamp), None):
                    map_timestamp_to_file_name[long(child_timestamp)].add(file)
                else:
                    map_timestamp_to_file_name[long(child_timestamp)] = set([file])

        sorted_timestamp.sort()
        print ("sorted_timestamp=%s" % sorted_timestamp)
        logger.debug("sorted_timestamp=%s" % sorted_timestamp)

        for t in sorted_timestamp:
            if self.last_file_name: 
                if t <= self.last_file_name:
                    if 1 == debug_flag:
                        logger.debug("ignore this timestamp=%s, last=%s" % (t, self.last_file_name))
                        print("ignore this timestamp=%s, last=%s" % (t, self.last_file_name))
                    continue
            
            file_name_set = map_timestamp_to_file_name[t]
            for file_name in file_name_set: 
                child = os.path.join('%s/%s' % (self.directory, file_name)) 
                self.file_array.append(child)
                logger.info("add file:%s" % child)

        if not self.file_array:
            self.is_read_end = True
            logger.info("no file exists:[%s]" % self.directory)
            return None, type 
        if self.remain_num and self.remain_num > 0 and len (self.file_array) > self.remain_num:
            #跳过跟不上的历史数据
            self.index = len(self.file_array) - self.remain_num 
            logger.info("remain_num=%s, len_file_array=%s, index start=%s" % (self.remain_num, len(self.file_array), self.index))
            print("remain_num=%s, len_file_array=%s, index start=%s" % (self.remain_num, len(self.file_array), self.index))

        file = self.file_array[self.index]
        self.index = self.index + 1
        type = self.get_suffix_type(file)
        return file, type 
    
    def get_suffix_type(self, name):
        if not name:
            return ''
        pattern = re.compile(r'\.(mv|new|new_part_\d+)$') #注意正则，不同文件名格式不一样
        group_match = pattern.findall(name)
        logger.debug("file=%s, group_match=%s" % (file, group_match))
        if group_match and isinstance(group_match, list):
            return group_match[0]
        return group_match 

    def get_last_file_name(self):
        if self.force_sync:
            logger.debug("not to get last file:%s" % self.store_last_file)
            return
        if (os.path.exists(self.store_last_file)):
            if (os.path.getsize(self.store_last_file) > 0):
                time_file = open(self.store_last_file, "r")
                self.last_file_name = time_file.read()
                time_file.close()
                if self.last_file_name:
                    self.last_file_name = long(self.last_file_name)
            else:
                logger.debug("empty file:%s" % self.store_last_file)
        else:
            logger.debug("no such file:%s" % self.store_last_file)
    

    def record_last_file_name(self, file_name):
        if not file_name:
            logger.error("no rule emtpy match") 
            return None
        pattern = re.compile(r'(\d+)\.(?:mv|new|new_part_\d+)$') #注意正则，不同文件名格式不一样
        group_match = pattern.findall(file_name)
        if not group_match:
            logger.error("no rule match??[%s]" % file_name)
            return None
        t = group_match[0]
        logger.info("file_name=[%s] timestamp=[%s]" % (file_name, t))
        
        time_file = open(self.store_last_file, "w")
        time_file.write("%s" % t)
        time_file.close()

class CEntityItem(object):
    def __init__(self):
        self.MD = None

    def from_dict(self, dict, is_gbk = True, ignore_zero_value = False):
        if not dict:
            logger.error("empty dict")
            return
        for k, v in dict.items():
            if v:
                uv = None
                if is_gbk: 
                    try:
                        uv = v.decode('gbk')
                    except Exception,e:
                        logger.error("error line:[%s] [%s] [%s] " % (k, v, dict["MD"]))
                        logger.error(traceback.format_exc())
                        raise Exception("decode error")
                else:
                    uv = v
                if v == '0' and ignore_zero_value:
                    continue
                self.__setattr__(k, uv)
            else:
                self.__setattr__(k, v)

class data_sync_field_def_line(object):
    def __init__(self):
        self.c_sync_field = None
        self.c_sync_name = None
        self.c_table_name = None
        self.c_sequence = 0

    def from_row(self, r):
        if not r:
            logger.error("invalid parameters? r=%s" % r)
            return
        self.c_sync_field = r['c_sync_field']
        self.c_sync_name = r['c_sync_name'] 
        self.c_table_name = r['c_table_name'] 
        self.c_sequence = r['c_sequence'] 
        return

class CDataSaver2Db(object):
    def __init__(self):
        self.table_prefix_2_db_prefix = {'t_direct_dynamic_' : 'd_complex_db_dynamic_', 't_direct_static_' : 'd_complex_db_static_'}

        self.map_fields_to_v3_protocol = {} #table_name => {c_id_md5 => MD}
        self.map_v3_protocol_to_fields = {} #table_name => {MD => c_id_md5}
        self.sequence_line_array = [] #数组元素为data_sync_field_def_line类,按照c_sequence从小到大排序

        sql = "select * from t_data_sync_field_def"
        rows = DBQueryPool.get_instance("d_complex_task").get_dict(sql)
        for r in rows:
            oo = data_sync_field_def_line()
            oo.from_row(r)
            self.sequence_line_array.append(oo)

            c_table_name = r['c_table_name']
            c_sync_field = r['c_sync_field']
            c_sync_name = r['c_sync_name']
            if not c_table_name or not c_sync_field or not c_sync_name:
                logger.error("invalid item from t_data_sync_field_def:[%s]" % r)
                print("#######invalid item from t_data_sync_field_def:[%s]" % r)
                continue
            if self.map_fields_to_v3_protocol.get(c_table_name):
                table_dict = self.map_fields_to_v3_protocol[c_table_name]
                table_dict[c_sync_name] = c_sync_field
            else:
                table_dict = {}
                table_dict[c_sync_name] = c_sync_field
                self.map_fields_to_v3_protocol[c_table_name] = table_dict 

            if self.map_v3_protocol_to_fields.get(c_table_name):
                table_dict = self.map_v3_protocol_to_fields[c_table_name] 
                table_dict[c_sync_field] = c_sync_name
            else:
                table_dict = {}
                table_dict[c_sync_field] = c_sync_name
                self.map_v3_protocol_to_fields[c_table_name] = table_dict

        self.sequence_line_array.sort(key = lambda o:long(o.c_sequence))

        if 1 == debug_flag:
            for table, dict in self.map_fields_to_v3_protocol.items():
                print "table=%s, dict = \n%s\n" % (table, '\n' . join(["%s:%s" % (k, v) for k, v in dict.items()]))
                logger.info("table=%s, dict = \n%s\n" % (table, '\n' . join(["%s:%s" % (k, v) for k, v in dict.items()])))

    def _get_table_name_from_md(self, table_name, MD):
        db_prefix = "d_complex_db_static_" 
        if table_name == 't_direct_dynamic_':
            db_prefix  = 'd_complex_db_dynamic_'
        offset = rs_hash(MD) % 200
        db_offset = offset/50 
        real_table = "%s%s.%s%s" % (db_prefix, db_offset, table_name, offset)
        logger.debug("offset=%s, db_offset=%s, real_table=%s" % (offset, db_offset, real_table)) 
        if 1 == debug_flag:
            print("offset=%s, db_offset=%s, real_table=%s" % (offset, db_offset, real_table)) 
        return real_table 

    def _get_table_name_from_idx(self, table_name, idx):
        db_prefix = "d_complex_db_static_" 
        if table_name == 't_direct_dynamic_':
            db_prefix = 'd_complex_db_dynamic_'
        offset = idx
        db_offset = offset/50 
        real_table = "%s%s.%s%s" % (db_prefix, db_offset, table_name, offset)
        logger.debug("offset=%s, db_offset=%s, real_table=%s" % (offset, db_offset, real_table)) 
        if 1 == debug_flag:
            print("offset=%s, db_offset=%s, real_table=%s" % (offset, db_offset, real_table)) 
        return real_table 


    def delete_data(self, obj_list):
        if not obj_list:
            return

        MD_set = set()
        for o in obj_list:
            MD_set.add(o.MD)

        table_number_2_md_list = get_table_number_dict(MD_set, 200)
        if 1 == debug_flag:
            print "table_number_2_md_list=%s" % table_number_2_md_list 
        tables = self.map_fields_to_v3_protocol.keys()
        for idx, md_list in table_number_2_md_list.items(): 
            if not md_list: 
                continue
            for t in tables: 
                table = self._get_table_name_from_idx(t, idx)
                sql = "delete from %s where c_id_md5 in (%s) " %\
                        (table, ',' . join(["'%s'" % md for md in md_list]))
                afects = DBQueryPool.get_instance("d_complex_db_static").execute_sql(sql)

    def insert_data_only(self, obj_list, entity_type):
        if not obj_list:
            logger.error("not to insert_data_only for empty list")
            return
        tables = self.map_fields_to_v3_protocol.keys()
        table_num_2_obj_list = get_table_number_dict_for_obj(obj_list, 200)
        for idx, o_list in table_num_2_obj_list.items():
            for t in tables: 
                dict = self.map_fields_to_v3_protocol[t]
                fields_list = set(dict.keys()) - set(['c_id_md5', 'MD'])
                insert_value_list = []
                for o in o_list:
                    for field in fields_list: 
                        if not hasattr(o, dict[field]):
                            o.__setattr__(dict[field], "")
                            continue

                    insert_values = ""
                    if not o.MD:
                        logger.debug("ignore to update for no MD")
                        continue

                    insert_values = "%s, %s" % (o.MD, ',' . join(["'%s'" % (MySQLdb.escape_string(o.__getattribute__(dict[field_name]))) \
                                                for field_name in fields_list]))
                    if not insert_values: 
                        logger.error("ignore to do insert/udpate, %d", len(obj_list))
                        continue
                    insert_value_list.append(insert_values)
                    #print "insert_values = %s\nupdate_values=%s\n" % (insert_values, update_values) 

                sql = "insert ignore into %s (c_id_md5, %s) values %s "\
                        %(self._get_table_name_from_idx(t, idx), ',' . join(["%s" % field for field in fields_list]), \
                          ', ' . join(["(%s)" % v for v in insert_value_list]))
                afects = DBQueryPool.get_instance("d_complex_db_static").execute_sql(sql)

    def save_data(self, obj_list, entity_type):
        if not self.map_fields_to_v3_protocol:
            logger.error("no map_fields_to_v3_protocol")
            return False
        tables = self.map_fields_to_v3_protocol.keys()
        if 'both' != entity_type:
            tables = ['t_direct_dynamic_']
        my_redis = CRedis(self)

        for t in tables: 
            dict = self.map_fields_to_v3_protocol[t]
            standard_fields = set(dict.keys()) - set(['c_id_md5', 'MD'])
            if t == 't_direct_dynamic_':
                my_redis.save_data(obj_list)
                continue
            for o in obj_list:
                fields_list = []
                for field in standard_fields: 
                    if not hasattr(o, dict[field]):
                        continue
                    fields_list.append(field) 

                insert_values = ""
                update_values = ""
                if not o.MD:
                    logger.debug("ignore to update for no MD")
                    continue

                insert_values = ',' . join(["'%s'" % (MySQLdb.escape_string(o.__getattribute__(dict[field_name]))) \
                                            for field_name in fields_list])
                update_values = ',' . join(["%s='%s'" % (field_name, (MySQLdb.escape_string(o.__getattribute__(dict[field_name])))) \
                                            for field_name in fields_list])

                if not insert_values or not update_values:
                    logger.error("ignore to do insert/udpate, %d", len(obj_list))
                    continue
                #print "insert_values = %s\nupdate_values=%s\n" % (insert_values, update_values) 
                sql = "insert into %s (c_id_md5, %s) values(%s, %s) on duplicate key update %s"\
                        %(self._get_table_name_from_md(t, o.MD), ',' . join(["%s" % field for field in fields_list]), \
                          o.MD, insert_values, update_values)

                afects = DBQueryPool.get_instance("d_complex_db_static").execute_sql(sql)

        return True

class CDataReader(CDataSaver2Db):
    def __init__(self):
        CDataSaver2Db.__init__(self)
        self.max_count_when_dump_all = 120000
        self.recorder_dir_prefix = None
        if 1 == debug_flag:
            self.max_count_when_dump_all = 2
            for table, dict in self.map_v3_protocol_to_fields.items():
                print "**table=%s, dict = \n%s\n" % (table, '\n' . join(["%s:%s" % (k, v) for k, v in dict.items()]))
                logger.info("**table=%s, dict = \n%s\n" % (table, '\n' . join(["%s:%s" % (k, v) for k, v in dict.items()])))
            
            for oo in self.sequence_line_array:
                print "##line=%s" % (',' . join (['%s:%s' % (k, v) for k, v in oo.__dict__.items()]))
                logger.debug("##line=%s" % (',' . join (['%s:%s' % (k, v) for k, v in oo.__dict__.items()])))
    def setRecorderFilePrefix(self, dir):
        self.recorder_dir_prefix = dir

    def load_all_data(self):
        """
        导出全量数据
        """
        table_2_max = {}
        table_2_min = {}
        range_max = 200 
        if 1 == debug_flag:
            range_max = 3

        table_prefix = 't_direct_static_'
        db_prefix = 'd_complex_db_static_'
        map_static_table_2_dynamic_table = {}

        my_redis = CRedis(self)
        for i in range(0, range_max):
            db_offset = i/50
            table_name = "%s%s.%s%s" % (db_prefix, db_offset, table_prefix, i)
            dynamic_table = "d_complex_db_dynamic_%s.%s%s" % (db_offset, 't_direct_dynamic_', i)
            map_static_table_2_dynamic_table [table_name] = dynamic_table 
            min_id, max_id = self.get_id_range(table_name)
            if max_id and min_id and max_id >= min_id:
                table_2_max[table_name] = max_id
                table_2_min[table_name] = min_id
            if 1 == debug_flag:
                print ("max/min=%s:%s, for table=%s" % (max_id, min_id, table_name))
        sufix_n = 0
        for table, max_id in table_2_max.items():
            obj_list = []
            min_id = table_2_min[table]
            len = max_id - min_id 
            if len > self.max_count_when_dump_all:
                step = 1 + len/self.max_count_when_dump_all
                for start_idx in range(0, step):
                    where_condition = None
                    start = min_id + start_idx*self.max_count_when_dump_all
                    end   = min_id + (1+start_idx)*self.max_count_when_dump_all
                    if end >=  max_id: 
                        where_condition = "where %s.c_id >= '%s'" % (table, start)
                    else:
                        where_condition = "where %s.c_id >= '%s' and %s.c_id < '%s' " % (table, start, table, end)

                    obj_list = self.read_from_one_table(table, None, where_condition)
                                                        #map_static_table_2_dynamic_table[table], where_condition) 
            else:
                obj_list = self.read_from_one_table(table, None, None) 
                #map_static_table_2_dynamic_table[table], None) 

            my_redis.merge_from_redis(obj_list) 
            if 1 == debug_flag:
                print_debug_info(obj_list)
            if not self.recorder_dir_prefix: 
                logger.error("no directory?")
                raise Exception("no directory?")
            if sufix_n > 35:
                sufix_n = 0
            file_name = "%s.new_part_%s" % (self.recorder_dir_prefix, sufix_n)
            if sufix_n < 10:
                file_name = "%s.new_part_0%s" % (self.recorder_dir_prefix, sufix_n)
            recorder = CRecord2File(self)
            if False == recorder.record_2_file(obj_list, file_name):
                raise Exception("record_2_file on error?")
            sufix_n = sufix_n + 1

    def get_id_range(self, table_name):
        sql = "select min(c_id) as min_id, max(c_id) as max_id from %s " % (table_name)
        rows = DBQueryPool.get_instance("d_complex_db_static").get_dict(sql)
        if not rows:
            logger.info("no min/max =%s" % table_name)
            return 0, 0
        
        min_id, max_id = rows[0]['min_id'], rows[0]['max_id']
        logger.info("%s, min/max =%s:%s" % (table_name, min_id, max_id))
        return long(min_id), long(max_id)

    def read_from_one_table(self, table, dynamic_table, where_condition):
        if not where_condition:
            where_condition = ""
        sql = "select * from %s %s" % (table, where_condition) 
        if dynamic_table:
            sql = "select * from %s left join %s on (%s.c_id_md5 = %s.c_id_md5) %s" % \
                    (table, dynamic_table, table, dynamic_table, where_condition)

        rows = DBQueryPool.get_instance("d_complex_db_static").get_dict(sql)
        o_list = []
        for r in rows:
            if 0 != r['c_is_quality']:
                if 1 == debug_flag:
                    logger.error("ignore item[%s]" % r['c_id_md5'])
                continue
            o = CEntityItem()
            o.from_dict(r, False)
            o_list.append(o)
        return o_list

class CRecord2File(object):
    def __init__(self, reader):
        self.table_prefix_2_db_prefix = reader.table_prefix_2_db_prefix 
        self.map_fields_to_v3_protocol = reader.map_fields_to_v3_protocol  #table_name => {c_id_md5 => MD}
        self.map_v3_protocol_to_fields = reader.map_v3_protocol_to_fields  #table_name => {MD => c_id_md5}
        self.sequence_line_array = reader.sequence_line_array #数组元素为data_sync_field_def_line类,按照c_sequence从小到大排序
    def record_2_storage(self, obj_list, file_name):
        return self.record_2_file(obj_list, file_name)

    def record_2_file(self, obj_list, file_name):
        outfile = open(file_name, "a")
        logger.debug("record_2_file=%s, len(obj_list)=%s" % (file_name, len(obj_list)))
        if not obj_list:
            outfile.close()
            return True
        if not self.sequence_line_array: 
            logger.error("record_2_file=%s, len(obj_list)=%s" % (file_name, len(obj_list)))
            raise Exception("invalid parameters??")

        if os.path.getsize(file_name)>0:
            outfile.write("\n")
        outfile.write("!!\n")
        for o in obj_list:
            for seq in self.sequence_line_array:
                field_name = seq.c_sync_name
                field_value = None
                if hasattr(o, field_name):
                    field_value = "%s" % o.__getattribute__(field_name)

                if field_value :
                    field_value.replace("\n", "")
                    field_value.replace("\r", "")

                v3_protocol_key = None
                for table, dict in self.map_fields_to_v3_protocol.items():
                    v3_protocol_key = dict.get(field_name, None)
                    if v3_protocol_key:
                        break
                    
                if not v3_protocol_key:
                    logger.error("invalid field_name? write field_value, field_name=%s, v3_protocol_key=%s, field_value=%s" % (field_name, v3_protocol_key, field_value))
                    continue

                outfile.write(v3_protocol_key)
                if field_value is not None:
                    try:
                        field_value.decode("utf8")
                        field_value.encode("gbk")
                        outfile.write(field_value)
                    except:
                        logger.error("write field_value, field_name=%s, v3_protocol_key=%s, field_value=%s" % (field_name, v3_protocol_key, field_value))
                        outfile.write("")
                outfile.write("\n")

            outfile.write("\n")
        outfile.close()
        return True

class CRedis(CRecord2File):
    def __init__(self, reader):
        CRecord2File.__init__(self, reader)
        self.my_redis = CRedisHelper()

    def save_data(self, obj_list, ignore = None):
        return self.record_2_redis(obj_list)

    def delete_data(self, obj_list):
        for o in obj_list:
            key = self.build_redis_key(o)
            if key:
                self.my_redis.delete_key(key) 
                if 1 == debug_flag:
                    print "delete key=%s" % (key)
            else:
                logger.error("not to delete key:[%s] [%s] " % (o.MD, key))

    def record_2_storage(self, obj_list, file_name):
        return self.record_2_redis(obj_list)
    
    def build_redis_key(self, o):
        return "complex_dynamic_info_%s" % o.MD

    def build_redis_value(self, o):
        d = {}
        keys = self.map_v3_protocol_to_fields['t_direct_dynamic_'].keys()
        for k in keys:
            if hasattr(o, k):
                d[k] = o.__getattribute__(k)
        if d:  
            update_time = time.asctime(time.localtime(time.time()))
            value = {'data' : d}
            value["update_time"] = update_time 
            json_value = json.dumps(value)
            return json_value 
        return None

    def record_2_redis(self, obj_list):
        for o in obj_list:
            key = self.build_redis_key(o)
            val = self.build_redis_value(o)
            if key and val:
                self.my_redis.set_value(key, val) 
                if 1 == debug_flag:
                    print "key=%s, value=%s" % (key, val)
            else:
                logger.error("not to set value:[%s] [%s] [%s]" % (o.MD, key, val))

    def merge_data_2_obj(self, val, o):
        if not val:
            #logger.error("empty val from redis:[%s] " % o.MD)
            return
        try:
            decode_data = json.loads(val)
            data = decode_data.get('data', None)
            if not data:
                logger.error("no data node; MD[%s], val[%s]" % (o.MD, val))
                return
            keys = self.map_v3_protocol_to_fields['t_direct_dynamic_'].keys()
            for k in keys:
                if data.get(k, None) != None:
                    o.__setattr__(k, data[k])
                    continue
                o.__setattr__(k, 0)

        except Exception,e:
            logger.error(traceback.format_exc())
            logger.error("loads json data failed:[%s] for md[%s]" % (val, o.MD))

    def merge_from_redis(self, obj_list):
        for o in obj_list:
            key = self.build_redis_key(o)
            if key:
                val = self.my_redis.get_value(key) 
                if 1 == debug_flag:
                    print "get key=%s, value=%s" % (key, val)
                self.merge_data_2_obj(val, o)
            else:
                logger.error("not to get value:[%s] [%s] " % (o.MD, key))

def triggle_relative_task(obj_list):
    if not obj_list:
        return
    
    task = Task('t_data_push_incr_task', db_key='d_complex_task')
    map_IF_2_ID_list = {}
    for o in obj_list:
        if hasattr(o, 'IF') and hasattr(o, 'ID') and hasattr(o, 'IB') and hasattr(o, 'IA'):
            mid = o.ID
            type = 'video'
            if 2 == o.IF: 
                mid = o.IB
                type = 'cover'
            elif 3 == o.IF: 
                mid = o.IA
                type = 'column'

            if not mid: 
                logger.error("invalid id? o.id=%s, o.ib=%s, o.ia=%s, o.IF=%s" % (o.ID, o.IB, o.IA, o.IF))
                continue

            if map_IF_2_ID_list.get(type):
                map_IF_2_ID_list[type].append(mid)
            else:
                map_IF_2_ID_list[type] = [mid]

    for type, id_list in map_IF_2_ID_list.items():
        count = 500
        if 1 == debug_flag:
            count = 5

        max_len = len(id_list)
        step = 1 + len(id_list)/count
        if 1 == debug_flag:
            print "type=%s, id_list=%s" % (type, id_list)

        for i in range(0, step):
            start = i*count
            end = (1+i)*count
            if end >= max_len:
                end = max_len
            if end < start: 
                continue
            part_id_list = id_list[start:end]
            if 1 == debug_flag:
                print "len=%s, count=%s, max=%s, start=%s, end=%s, part_id_list=%s" % \
                        (len(id_list), count, step, start, end, part_id_list)
            logger.debug("len=%s, count=%s, max=%s, start=%s, end=%s, part_id_list=%s" % \
                         (len(id_list), count, step, start, end, part_id_list))

            if part_id_list: 
                task.insert_new_task(part_id_list, type, sys.argv[0])

def write_2_db(writer, obj_list, type, entity_type, push_flag, sync_all):
    if not obj_list:
        return
    if 'mv' == type: 
        print "enter type = %s" % type
        writer.delete_data(obj_list)
    elif sync_all:
        writer.insert_data_only(obj_list, entity_type)
    else:
        writer.save_data(obj_list, entity_type)
        if push_flag: 
            # 触发推送任务
            triggle_relative_task(obj_list)

def handle_one_file(file, type, entity_type, push_flag, sync_all):
    if not file:
        return -1
    if not os.path.isfile(file):
        logger.error("no such file:%s" % file)
        return -2
    max_sync_count = 5000
    if 1 == debug_flag:
        max_sync_count = 50
    elif sync_all:
        max_sync_count = 20000

    new = False
    obj_list = []
    dict = {}
    dbHelper = CDataSaver2Db()
    if 'dynamic_cache' == entity_type and not sync_all:
        dbHelper = CRedis(dbHelper)
    keys_set = set()
    for t, d in dbHelper.map_v3_protocol_to_fields.items():
        for k, v in d.items():
            keys_set.add(k)
    
    if not keys_set:
        logger.error("no keys_set")
        raise Exception("invalid def")
    logger.info("keys_set=%s" % keys_set)

    for line in open(file,"r"):
        if len(line)>0:
            line = line.strip()
            if not line:
                if new and dict:
                    o = CEntityItem()
                    o.from_dict(dict)
                    obj_list.append(o)
                    dict = {}
                new = False
                if len(obj_list) > max_sync_count: 
                    write_2_db(dbHelper, obj_list, type, entity_type, push_flag, sync_all)
                    obj_list = []
                    if 1 == debug_flag:
                        break
                continue 
            key = line[0:2]
            #一条的开始
            if key=="!!":
                dict = {}
                new=True
                continue
            if key not in keys_set:
                #logger.error("invalid line:[%s], k[%s], keys_set[%s]" % (line, key, keys_set))
                continue

            if new:
                value = line[2:]
                dict[key] = value

    if dict and new:
        o = CEntityItem()
        o.from_dict(dict)
        obj_list.append(o)

    if 1 ==debug_flag:
        print_debug_info(obj_list)
    write_2_db(dbHelper, obj_list, type, entity_type, push_flag, sync_all)
    obj_list = []

################################################
def read_file_to_dict(dict, file, keys_set):
    if not file:
        return -1
    if not os.path.isfile(file):
        logger.error("no such file:%s" % file)
        return -2

    new = False
    obj_list = []
    cur_dict = {}
    for line in open(file,"r"):
        if len(line)>0:
            line = line.strip()
            if not line:
                if new and cur_dict :
                    o = CEntityItem()
                    o.from_dict(cur_dict )
                    dict[o.__getattribute__("MD")] = o
                    cur_dict = {}
                new = False
                continue 
            key = line[0:2]
            #一条的开始
            if key=="!!":
                cur_dict = {}
                new=True
                continue
            if key not in keys_set:
                #logger.error("invalid line:[%s], k[%s], keys_set[%s]" % (line, key, keys_set))
                continue

            if new:
                value = line[2:]
                cur_dict[key] = value


def write_record(outfile, file, key_set, o, add_0 = True):
    if os.path.getsize(file)>0:
        outfile.write("\n")
    outfile.write("!!\n")
    for field in key_set:
        outfile.write(field)
        try:
            if hasattr(o, field):
                outfile.write(o.__getattribute__(field))
            else:
                outfile.write("0")
        except:
            logger.error("write failed:%s, field=%s" % (o.MD, field))
            outfile.write("")
        outfile.write("\n")

    outfile.write("\n")

def split_write_back_to_file(key_set, obj_list, file_name_array):
    if not obj_list or not file_name_array:
        logger.error("no data:len of obj=%s, len of array=%s" % (len(obj_list), len(file_name_array)))
        return
    dict = {}
    split_num = len(file_name_array)
    for o in obj_list:
        mod_num = rs_hash(o.MD) % split_num
        file_name = file_name_array[mod_num]
        if dict.get(file_name, None):
            dict[file_name].append(o)
        else:
            dict[file_name] = [o]

    for file_name, o_list in dict.items():
        if o_list:
            outfile = open(file_name, "a")
            for o in o_list:
                write_record(outfile, file_name, key_set, o)
            outfile.close() 
            logger.debug("write file:%s, lines=%s" % (file_name, len(o_list)))

def split_files(key_set, file, target_dir, max_file_size, split_num):
    """
    拆分文件，如果大于max_file_size(MB)
    """
    if not target_dir:
        logger.error("invalid directory:%s, %s" % (target_dir, file))
        raise Exception("invalid directory:%s, %s" % (target_dir, file))

    max_file_size = max_file_size*1024*1024
    if not os.path.isfile(file):
        logger.error("no such file:%s" % file)
        return -1

    cur_file_size = os.path.getsize(file)
    #if cur_file_size  <= (3*max_file_size/2):
    #    logger.debug("not to split this file:%s, size=%s, max_file_size=%s" % (file, cur_file_size, max_file_size))
    #    return 0

    logger.debug("split num:file=%s, size=%s, max_file_size=%s, split_num=%s" % (file, cur_file_size, max_file_size, split_num))
    expression = r'(\d+)\.(?:new)$' #注意正则，不同文件名格式不一样
    pattern = re.compile(expression)
    group_match = pattern.findall(file)
    if group_match: 
        logger.debug("file=%s, group_match=%s" % (file, group_match))
        if 1 == debug_flag:
            print("file=%s, group_match=%s" % (file, group_match))
    else:
        logger.debug("file=%s, group_match is null=%s" % (file, group_match))
        return -2
    time_stamp_start = long(group_match[0])
    file_name_array = []
    for i in range(0, split_num):
        file_name_array.append("%s/%s.new" % (target_dir, time_stamp_start+i))
    logger.debug("file_name_array=%s" % file_name_array)

    cur_dict = {}
    obj_list = []
    new = False
    max_lines = 100000
    for line in open(file,"r"):
        if len(line)>0:
            line = line.strip()
            if not line:
                if new and cur_dict :
                    o = CEntityItem()
                    o.from_dict(cur_dict )
                    obj_list.append(o)
                    cur_dict = {}
                    if len(obj_list) > max_lines:
                        split_write_back_to_file(key_set, obj_list, file_name_array)
                        obj_list = []
                new = False
                continue 
            key = line[0:2]
            #一条的开始
            if key=="!!":
                cur_dict = {}
                new=True
                continue
            if key not in key_set:
                #logger.error("invalid line:[%s], k[%s], keys_set[%s]" % (line, key, keys_set))
                continue

            if new:
                value = line[2:]
                cur_dict[key] = value

    if obj_list:
        split_write_back_to_file(key_set, obj_list, file_name_array)

def merge_obj(o, dict):
    another = dict.get(o.MD, None)
    if not another:
        return o
    if not hasattr(another, 'SB'): 
        return o
    if not hasattr(o, "SB"):
        return another

    anther_SB = long(another.SB) 
    o_SB = long(o.SB)
    if anther_SB > o_SB:
        return another
    return o

def get_specified_merged_file(file_name, split_num, merge_result_file):
    if not file_name:
        logger.error("file_name is NULL")
        return None, 0

    r_file = open(file_name, "r")
    for line in r_file:
        if len(line)>0:
            line = line.strip()
            key = line[0:2]
            #一条的开始
            if key=="!!":
                cur_dict = {}
                new=True
                continue
            if key and key == 'MD':
                value = line[2:]
                MD = value
                mod_num = rs_hash(MD) % split_num
                print ("file=%s, split_num=%s, mod=%s" % (file_name, split_num, mod_num))
                logger.info("file=%s, split_num=%s, mod=%s" % (file_name, split_num, mod_num))
                r_file.close()
                return "%s_%s" % (merge_result_file, mod_num), mod_num

    r_file.close()
    logger.info("file=%s, split_num=%s, close file" % (file_name, split_num))
    return None, 0

###merge
def do_merge_2_files(key_set, file1, file2, merge_result_file, split_num, code ):
    logger.debug("merge 2 files:[%s] [%s] target[%s]" % (file1, file2, merge_result_file))
    if not os.path.exists(file2) or os.path.getsize(file2) == 0:
        if not os.path.exists(file1) or os.path.getsize(file1) == 0:
            logger.error("no such 2 files:%s, %s" % (file1, file2))
            return
        os.rename(file1, merge_result_file)
        logger.debug("rename from [%s] to [%s]" % (file1, merge_result_file))
        return
    elif not os.path.exists(file1) or os.path.getsize(file1) == 0:
        os.rename(file2, merge_result_file)
        logger.debug("rename from [%s] to [%s]" % (file2, merge_result_file))
        return

    min_file = file1
    max_file = file2
    if (os.path.getsize(min_file) > os.path.getsize(max_file)):
        min_file = file2;
        max_file = file1;
    
    #load比较小的文件到内存
    min_dict = {}
    read_file_to_dict(min_dict, min_file, key_set)
    #读取大的文件，读固定的行数，然后写入文件
    new = False
    obj_list = []
    cur_dict = {}
    max_objs_cnt = 500
    writed_id_set = set()
    outfile = open(merge_result_file, "a")
    first_print = True
    if 0 != code or 1 != debug_flag:
        first_print = False

    for line in open(max_file,"r"):
        if len(line)>0:
            line = line.strip()
            if not line:
                if new and cur_dict :
                    o = CEntityItem()
                    o.from_dict(cur_dict, False, True)
                    o = merge_obj(o, min_dict)
                    mod_num = rs_hash(o.MD) % split_num
                    if mod_num != code:
                        logger.error("invalid mod_num=%s, code=%s, MD=%s, file_name=[%s] [%s]" % (mod_num, code, o.MD, file1, file2))
                        print("invalid mod_num=%s, code=%s, MD=%s, file_name=[%s] [%s]" % (mod_num, code, o.MD, file1, file2))
                        raise Exception("invalid mod_num=%s, code=%s, MD=%s, file_name=[%s] [%s]" % (mod_num, code, o.MD, file1, file2))

                    obj_list.append(o)
                    if min_dict.get(o.MD):
                        writed_id_set.add(o.MD)

                    cur_dict = {}
                    if (len(obj_list) > max_objs_cnt):
                        if first_print:
                            first_print = False
                            print_debug_info(obj_list)

                        for o in obj_list:
                            write_record(outfile, merge_result_file, key_set, o)
                        #logger.debug("write file:%s, lines=%s" % (merge_result_file, len(obj_list)))
                        obj_list=[]

                new = False
                continue 
            key = line[0:2]
            #一条的开始
            if key=="!!":
                cur_dict = {}
                new=True
                continue
            if key not in key_set:
                #logger.error("invalid line:[%s], k[%s], key_set[%s]" % (line, key, key_set))
                continue

            if new:
                value = line[2:]
                cur_dict[key] = value

    outfile.close() 
    if (obj_list):
        outfile = open(merge_result_file, "a")
        if first_print:
            first_print = False
            print_debug_info(obj_list)

        for o in obj_list:
            write_record(outfile, merge_result_file, key_set, o)
        outfile.close() 
        logger.debug("write file:%s, lines=%s" % (merge_result_file, len(obj_list)))
        obj_list=[]

    not_writed_id_set = set(min_dict.keys()) - writed_id_set
    if not_writed_id_set: 
        for id in not_writed_id_set:
            obj_list.append(min_dict[id])

        if (obj_list):
            outfile = open(merge_result_file, "a")
            for o in obj_list:
                write_record(outfile, merge_result_file, key_set, o)
            outfile.close() 
            logger.debug("write file with not writed id set:%s, lines=%s" % (merge_result_file, len(obj_list)))
            obj_list=[]

def test():

    reader = CDataReader()
    my_redis = CRedis(reader)
    o = CEntityItem()
    o.MD = "7356622772859837685"
    obj_list = [o]
    my_redis.merge_from_redis(obj_list) 
    print_debug_info(obj_list)

def test2():
    #def __init__(self, new_file_dir,mv_file_dir, store_file):
    dhandler = CMergeTwoDirectorIterator('/data/zhijunluo/tenvideo_base/search_prj/complex_data_sync/output/dynamic_field', '/data/zhijunluo/tenvideo_base/search_prj/complex_data_sync/output/sync_input', sys.argv[0])
    last_mv_file = ''
    last_new_file = ''
    while(True):
            file, type = dhandler.get_one_new_file_to_read()
            print "get one file=%s, type=%s" % (file, type) 
            if type == 'mv':
                last_mv_file = file
            elif type == 'new':
                last_new_file = file

            if not file:
                break
    dhandler.record_last_file_name( last_mv_file )
    dhandler.record_last_file_name( last_new_file )

def main():
    test2()

if __name__ == "__main__":
    main()
