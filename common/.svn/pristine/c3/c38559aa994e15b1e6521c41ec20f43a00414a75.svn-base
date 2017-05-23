#!/usr/local/bin/python
#encoding=utf8
import sys
import logging
import time
import datetime
import commands
import traceback
import MySQLdb
from pprint import pprint
sys.path.append('../common')
sys.path.append('../conf')
from config import g_conf
from log import logger
import collections
import os
from os.path import join,getsize
reload(sys) 
sys.setdefaultencoding('utf-8')
from db import DBQueryPool
from read_v3_protocol_file_from_dir import CDiretoryIterator
import media_operator

debug_flag = 0
def format_data_for_json_for_test(val, defaut_val = ''):
    if val is None:
        return defaut_val 
    if isinstance(val, int):
        return val
    if isinstance(val, list):
        formated_list = []
        for e in val:
            formated_list.append(format_data_for_json_for_test(e, defaut_val))

        return formated_list
    new_val = val.replace('\t', '')
    new_val = val.replace('\n', '')
    new_val = val.replace(' ', '')
    return new_val 


class CTestHelper(object):
    def __init__(self):
        self.input_file_path = g_conf.COMPLEX_DATA_DUMP_CONF["TEST_FILE_EXIST_DIR"]
        self.time_stamp_file_name = "../var/%s_last_test_timestamp" % sys.argv[0]
        self.cur_timestamp = None
        self.vid_list = []
        self.cid_list = []
        self.vid_objs_list = []
        self.cid_objs_list = []

    def get_last_timestamp(self):
        time_file_path = self.time_stamp_file_name
        self.cur_timestamp = int(time.time())
        last_timestamp=None
        if (os.path.exists(time_file_path)):
            if (os.path.getsize(time_file_path) > 0):
                time_file = open(time_file_path, "r")
                last_timestamp = time_file.read()
                time_file.close()
        if not last_timestamp:
            last_timestamp = self.cur_timestamp
            logger.info("start_time is empty and reset to now")

        return last_timestamp 

    def save_cur_timestamp(self):
        #return #TODO: for test 
        if self.cur_timestamp:
            time_file = open(self.time_stamp_file_name, "w")
            time_file.write("%d" % self.cur_timestamp)
            time_file.close()
            logger.info("write file[%s] time[%s] " % (self.time_stamp_file_name, self.cur_timestamp))
        else:
            logger.error("not to save timestamp:[%s] " % self.cur_timestamp)

    def handle_one_file(self, file):
        if 1 == debug_flag:
            print ("gona handle one file:[%s]" % file)
        if not file:
            return -1
        if not os.path.isfile(file):
            logger.error("no such file:%s" % file)
            return -2

        logger.info("gona handle one file:[%s]" % file)
        new = False
        dict = {}
        for line in open(file,"r"):
            if len(line)>0:
                line = line.strip()
                if not line:
                    if new and dict:
                        o = media_operator.CSosoProtocolV3()
                        o.from_dict(dict)
                        if not self.add_one_obj_to_self_list(o):
                            logger.info("skip this file:[%s]" % (file))
                            break
                        dict = {}
                    new = False
                    continue 
                key = line[0:2]
                #一条的开始
                if key=="!!":
                    dict = {}
                    new=True
                    continue
                #if key not in keys_set:
                    #logger.error("invalid line:[%s], k[%s], keys_set[%s]" % (line, key, keys_set))
                    #continue

                if new:
                    value = line[2:]
                    dict[key] = value

        if dict and new:
            o = media_operator.CSosoProtocolV3()
            o.from_dict(dict)
            self.add_one_obj_to_self_list(o)

    def add_one_obj_to_self_list(self, o):
        if not o:
            return True
        if 0 != int(o.IG):
            return False
        if o.YA and o.YA == '自拍':
            return True
        #if o.ID == 'b0020telyns':#TODO:for test
            #return False
        if int(o.IF) == 1:
            #TODO: just for dev
            if len(self.vid_objs_list) > 3000:
                if len(self.cid_objs_list) > 3000:
                    return False
                return True

            self.vid_objs_list.append(o)
        elif int(o.IF) == 2:
            #TODO: just for dev
            if len(self.cid_objs_list) > 3000:
                if len(self.vid_objs_list) > 3000:
                    return False
                return True

            self.cid_objs_list.append(o)
        else:
            return False
        return True

    def get_recent_objs(self):
        #last_timestamp = self.get_last_timestamp()
        self.vid_list = []
        self.cid_list = []
        self.vid_objs_list = []
        self.cid_objs_list = []
        directory = self.input_file_path 
        dhandler = CDiretoryIterator(directory, "%s_%s" % (sys.argv[0], "test_timestamp"), False, 0) 
        last_file = None
        while(True):
            file, type = dhandler.get_one_new_file_to_read()
            logger.info("get one file=%s, type=%s" % (file, type))
            if 1 == debug_flag:
                print "get one file=%s, type=%s" % (file, type) 
            if not file or not type:
                break
            if type != 'new':
                continue
            self.handle_one_file(file)
            last_file = file

        dhandler.record_last_file_name(last_file)
        #self.save_cur_timestamp()
        for o in self.cid_objs_list:
            if o.IB:
                self.cid_list.append(o.IB)

        for o in self.vid_objs_list:
            if o.ID:
                self.vid_list.append(o.ID)

        logger.info("vids=%s, objs of vids=%s, cids=%s, objs of cids=%s" % \
                    (len(self.vid_list), len(self.cid_list), len(self.vid_objs_list), len(self.cid_objs_list)))
        return self.vid_list, self.cid_list
    
    def save_diff_to_db(self, id1, entity_type, handle_result, attr, old_value, new_value):
        logger.debug("id1=%s,entity_type=%s, handle_result=%s, check_id=%s, field=%s, old_val=[%s], new_val=[%s]" % \
                     (id1, entity_type, handle_result, self.check_id, attr, old_value, new_value))
        if old_value is not None:
            old_value = "%s" % old_value
        
        if new_value is not None:
            new_value = "%s" % new_value 

        sql = "insert into Search.t_complex_data_diff(c_entity_id, c_entity_type, c_check_id, c_diff_key, c_handle_resulte, "\
                "c_old_value, c_new_value, c_create_time) values ('%s', '%s', '%s', '%s', '%s', '%s', '%s', now())" %\
                (id1, entity_type, self.check_id, attr, handle_result if handle_result is not None else '' , \
                 MySQLdb.escape_string(old_value) if old_value is not None else '', MySQLdb.escape_string(new_value) if new_value is not None else '')

        afects = DBQueryPool.get_instance("Search").execute_sql(sql)
        return afects 

    
    def diff_info_and_save_to_db(self, entity_type, refactor_info_dict):
        if not refactor_info_dict or entity_type not in set(['cover', 'video']):
            return
        objs = self.cid_objs_list
        if entity_type == 'video':
            objs = self.vid_objs_list

        logger.info("entity_type=%s, len of refactor_info_dict=%s, online_len=%s" % (entity_type, len(refactor_info_dict.keys()), len(objs)))
        for old_obj in objs:
            self.check_id = int(time.time())
            new_obj = refactor_info_dict.get(old_obj.MD, None)
            if not new_obj: 
                self.save_diff_to_db(old_obj.get_id(), entity_type, 'no new data', None, None, None)
                continue
            if not old_obj.get_id():
                print "no id exists??\n#################old obj#################\n"
                old_obj.print_debug_info()
                print "no id exists??\n#################new obj#################\n"
                new_obj.print_debug_info()

            for k in  media_operator.CSosoProtocolV3.KYES:
                if k in set(['ZZMTIME', 'PF', 'PL', 'AA', 'AB', 'AC', 'SB', 'SC', 'SD', 'SE', 'SF',\
                             'WA', 'WB', 'WC', 'WD', 'SA']):
                    continue
                old_val = old_obj.__getattribute__(k)
                new_val = new_obj.__getattribute__(k)
                if old_val == new_val:
                    continue
                if isinstance(old_val, int) or isinstance(new_val, int):
                    old_obj.transform_to_int(k)
                    new_obj.transform_to_int(k)
                old_val = old_obj.__getattribute__(k)
                new_val = new_obj.__getattribute__(k)
                if old_val == new_val:
                    continue
                if old_val is None:
                    if new_val is None or new_val == '':
                        continue

                if new_val is None:
                    if old_val == '':
                        continue

                if k == 'DC' and old_val and not new_val:
                    continue
                if k == 'DD' and new_val:
                    continue
                if k == 'DA' and old_val == '0' and not new_val:
                    continue
                if k in set(['YF', 'YA', 'YB', 'YE', 'RB']) and new_val and not old_val:
                    continue
                if k in set(['CA', 'CB', 'CC']):
                    old_val = format_data_for_json_for_test(old_val)
                    new_val = format_data_for_json_for_test(new_val)
                    if old_val == new_val:
                        continue

                if (old_val is None or old_val == '') and new_val is not None and new_val:
                    continue
                self.save_diff_to_db(old_obj.get_id(), entity_type, 'diff', k, old_val, new_val)
