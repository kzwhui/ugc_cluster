#!/usr/bin/python
#encoding=utf8
import re
import pickle
import sys
import json
import httplib, urllib
import time,datetime
import logging
sys.path.append('/usr/local/zk_agent/names/')
from nameapi import getHostByKey
from log import logger
import media_shield
from boss_api import *

#logger=logging.getLogger("dbRootLogger")
debug_flag = 0

class MediaAlbumInterface(object):
    UNION_PRE_TID_SET = set()
    UNION_TID_LIST = []

    def __init__(self):
        self.mapTabidInfo = {}
        self.mapTypeToConfig = {}
        pass

    def set_media_id(self, mid):
        self.id = mid

    def get_type(self):
        return 0

    def get_id(self):
        return None

    def default_values(self):
        pass

    def is_shield(self):
        return False

    def get_class_type(self):
        return 0

    def set_extra_info(self, mapTabidInfo, mapTypeToConfig):
        self.mapTabidInfo = mapTabidInfo
        self.mapTypeToConfig = mapTypeToConfig

    @staticmethod
    def get_type_set(cls):
        return set()

class CTest(MediaAlbumInterface):
    UNION_TID_LIST = [605, 581]
    def __init__(self):
        MediaAlbumInterface.__init__(self)

class VplusInfo(MediaAlbumInterface):
    UNION_TID_LIST = [621]
    def __init__(self):
        MediaAlbumInterface.__init__(self)
        self.nick = None

class VVideoInfo(MediaAlbumInterface):
    UNION_TID_LIST = [622]
    def __init__(self):
        MediaAlbumInterface.__init__(self)
        self.deliverMethod = None
        self.video_uin = 0

class SpheniscidaeLiveInfo(MediaAlbumInterface):
    UNION_TID_LIST = [620]
    def __init__(self):
        MediaAlbumInterface.__init__(self)
        self.super_title = None
        self.title = None
        self.guest = None
        self.vid = None
        self.search_flag = None
        self.props_flag = None
        self.props_pay_flag = None
        self.stars = None
        self.sub_live_type = None

def print_debug_info(obj_list, attribute_name_list = None, extra_str = None, dump_all = False):
   print "/////////%s, len=%d, class=%s////////////////" % (extra_str if extra_str else "print_debug_info", len(obj_list), obj_list[0] if obj_list else 'None')
   cnt = 1
   for o in obj_list:
       cnt = cnt - 1
       if attribute_name_list:
           print "%s, id=%s, \n" % (' | ' . join(['%s:%s' % (attr, o.__getattribute__(attr)) for attr in attribute_name_list]), o.get_id())
           continue
       else:
           print "########################";
           print "objs =%s\n%s\n" % (o, '\n' . join(['%s:%s' % (k, (';' . join(["%s" % r for r in v]) if v and isinstance(v, list) else v)) for k, v in o.__dict__.items()]))
           print "************************\n";
       if cnt < 0 and not dump_all:
           break
   print "########################";

execfile('../conf/config.py')
class UnionHttp:
    """union cgi数据访问
    """

    def __init__(self):
        self.http_conf = g_conf.UNION_HTTP_CONF
        self.real_host = ""
        self.real_port = ""

    def get_conn(self):
        conn = None
        for loop in range(0, 1):
            try:
                logger.debug("Before httplib.HTTPConnection")
                self.real_host = self.http_conf['host']
                self.real_port = self.http_conf['port']
                ret, ip, port = getHostByKey(self.http_conf['host'])
                if ret == 0:
                    self.real_host = ip
                    self.real_port = port
                logger.debug("host=%s, port=%s" % (self.real_host, self.real_port))
                conn = httplib.HTTPConnection("%s:%s" % (self.real_host, self.real_port))
                logger.debug("After httplib.HTTPConnection")
                break
            except:
                attr_add(454323, 1)
                logger.error('[链接cgi(主机)失败]:' + str(self.http_conf))
                logger.error(traceback.format_exc())
            time.sleep(1)
        if conn:
            return conn
        alarm("[严重]无法连接union cgi",dead=True , alarm_level=g_conf.ALARM_LEVEL_SERVER_CONDB_FAIL)

    #对外唯一的接口，其它接口没有调试过，请验证再用, by zhijunluo
    def load_objects(self, cls, media_id, fault = False, mapTabidInfo = {}, mapTypeToConfig = {}):
        """
        fault: True表示如果获取不到数据，是正常的一种返回, 就会打印正常日志和减少重试
        """
        if not media_id:
            logger.error("request None Media_id" % (cls))
            return None
        o = None
        basic_sleep_time = 0
        fault_sleep_time = 0
        basic_retry_cnt = 2
        if fault:
            fault_sleep_time = basic_sleep_time 
            basic_retry_cnt = 1

        real_tid_list = cls.UNION_TID_LIST
        for tid in real_tid_list: 
            if basic_sleep_time > 0:
                time.sleep(basic_sleep_time)

            try_cnt = basic_retry_cnt 
            for c in range(0, try_cnt):
                cur_o = self.load_objects_with_tid(tid, cls, [media_id])
                if cur_o:
                    break
                if not fault:
                    logger.error("failed to get tid:[%d] [%s] try_cnt:[%d]" % (tid, media_id, c))
                    print("failed to get tid:[%d] [%s] try_cnt:[%d]" % (tid, media_id, c))
                else:
                    logger.debug("get tid with fault tor:[%d] [%s] try_cnt:[%d]" % (tid, media_id, c))

                if fault_sleep_time > 0:
                    time.sleep(fault_sleep_time*(1+c))

            if not cur_o: 
                if not fault:
                    logger.error("final failed to get tid:[%d] [%s] " % (tid, media_id))
                    print("final failed to get tid:[%d] [%s]" % (tid, media_id))
                    raise Exception("illegal result, no objs from union:[%s] " % tid)
                continue

            #print_debug_info(cur_o)
            if not o:
                o = cur_o[0] 
            else:
                for k, v in cur_o[0].__dict__.items():
                    if not hasattr(o, k):
                        o.__setattr__(k, v)
                        if 1 == debug_flag:
                            print("merge one kv:[%s] [%s] " % (k, v))

            if tid in cls.UNION_PRE_TID_SET:
                o.set_extra_info(mapTabidInfo, mapTypeToConfig)
                if o.is_shield():
                    logger.info("break for shield id")
                    #print("break for shield id")
                    break
                if 2 == o.get_class_type():
                    if media_shield.cover_shield_condition(o):
                        logger.info("break for shield cid")
                        #print("break for shield cid")
                        break
                elif 1 == o.get_class_type():
                    if media_shield.video_shield_comon_condition(o):
                        logger.info("break for shield vid")
                        #print("break for shield vid")
                        break

        if o:
            o.default_values()

        return o

    def load_objects_with_tid(self, tid, cls, id_list=[], need_filter_type=False):
        #print "id_list:%s", id_list
        objs=[]
        #count_per_one_time = 20
        count_per_one_time = 1
        request_times = (len(id_list) + count_per_one_time - 1) / count_per_one_time
        for i in range(request_times):
            url = self.get_request_url(tid, cls, id_list[i*count_per_one_time:(i+1)*count_per_one_time])
            try:
                objs += self._load_objects(cls, url)
            except Exception, e:
                logger.error(traceback.format_exc())
                logger.error("request url=[data.video.qq.com%s]" % url)
                raise Exception("load_objects from union fail")

        # 分类变更不会同步到倒排，有可能读取影视专辑的时候读取到综艺专辑
        #if need_filter_type:
        #    type_set = cls.get_type_set(cls)
        #    objs = [obj for obj in objs if obj.get_type() in type_set]
        return objs

    def get_request_url(self, tid, cls, id_list):
        if not id_list:
            logger.error("no id for tid=[%s] cls=[%s] " % (tid, cls))
            return ""
        url = "%s?%s&tid=%s&idlist=%s&otype=json" % (self.http_conf['cgi'], \
                self.http_conf['app_info'], tid, ','.join("%s" % id for id in id_list))
               # self.http_conf['app_info'], cls.UNION_TID_LIST , ','.join("%s" % id for id in id_list))
        return url

    def _load_objects(self, cls, url):
        objs=[]
        if not url:
            logger.error("no url:[%s" % cls)
            return objs
        conn = self.get_conn()
        resp = None
        try:
            conn.request("GET", url, headers = {"Host": "data.video.qq.com"})
            logger.debug("url:%s%s", self.real_host, url)
            resp = conn.getresponse()
            objs = self.parse_object_from_response_string(cls, resp.read())
        except Exception,e:
            logger.error("request fail, url:%s", url)
            logger.error(traceback.format_exc())
        finally:
            conn.close()
        if not resp or resp.status != 200:
            raise Exception("request fail, http status is %s" % resp.status)
        return objs

    def parse_object_from_response_string(self, cls, response_string):
        '''
        QZOutputJson={"errorno":0,"results":[{"fields":{"alias":[],"area_name":"内地","director":[],"leading_actor":[],"publish_date":null,"title":"循找","type":1},"id":"l06exe0q1f038qd","retcode":0}]};
        '''
        BEFORE_JSON_PART = "QZOutputJson="
        AFTER_JSON_PART = ";"
        if not response_string.startswith(BEFORE_JSON_PART) or not response_string.endswith(AFTER_JSON_PART):
            raise Exception("unknown response string format")
        json_string = response_string[len(BEFORE_JSON_PART):(len(response_string) - len(AFTER_JSON_PART))]
        return self.parse_object_from_json_string(cls, json_string)

    def parse_object_from_json_string(self, cls, json_string):
        root_json = json.loads(json_string, strict=False)
        if 'errorno' not in root_json or root_json['errorno'] != 0:
            raise Exception("parse object fail, errorno=%s" % root_json['errorno'])
        if 'excepts' in root_json:
            logger.error("union excepts=[%s]" % (root_json['excepts']))
            raise Exception("get union fail, excepts is not null")
        # now = datetime.datetime.now()
        # if now.microsecond % 3 == 1:
        #     raise Exception("get union fail, make test error, num=%s" % now.microsecond)
        #print root_json
        objs = []
        for o in root_json['results']:
            obj = cls()
            if 'id' not in o:
                raise Exception("illegal result, id is null")
            if 'retcode' not in o or o['retcode'] != 0:
                # 单个没有去掉，暂时忽略
                logger.error("retcode=[%s]" % (o['retcode']))
                continue
                raise Exception("error result, retcode=%s" % o['retcode'])
            if 'fields' not in o:
                raise Exception("illegal result, fields is null")
            obj.set_media_id(o['id'])
            for k,v in o['fields'].items():
                obj.__setattr__(k,v)
            objs.append(obj)
        return objs

def main():
    t = CTest()
    u = UnionHttp()
    obj = u.load_objects(CTest, "9e5hjtvqupqirv9") 
    print_debug_info([obj]) 

if __name__ == '__main__':
    main()
