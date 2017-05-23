#!/usr/bin/python
#encoding=utf8

import re
import pickle
import sys
import json
import httplib, urllib, urllib2
import time,datetime
import traceback
import logging
sys.path.append('/usr/local/zk_agent/names/')
from nameapi import getHostByKey
from db import DBQueryPool, DBQuery
logger=logging.getLogger("dbRootLogger")
execfile('../conf/config.py')

debug_flag=0

def attemp_get_host_from_zk(host, port):
    real_host = host
    real_port = port
    ret, ip, port = getHostByKey(host)
    if ret == 0:
        real_host = ip
        real_port = port
    return (real_host, real_port)

"""
CommonHttpInterface所需要的基础类
"""
class IHttpHelperInterface(object):
    INSERT_CGI_PATH = '/srt_tvbox'
    UPDATE_CGI_PATH = '/srt_tvbox'
    MEDIA_HTTP_CONF = 'TEST_HTTP_CONF' # 配置参照格式

    MEDIA_ID_NAME = 'test_id' # cgi的主键
    PRIMARY_ID_NAME = None    #当前obj的主键
    MEDIA_ID_NAME_IN_RESPONSE = None # 返回值的必须的主键

    INSERT_KEY_FIELDS = [] #插入的key, 当前类的属性的key
    UPDATE_KEY_FIELDS = [] #更新的key, 当前类的属性的key

    """
    下面3个是对cgi返回值的格式解释
    """
    RESPONSE_CODE_NAME = 'ret'
    RESPONSE_CODE_MSG = 'msg'
    RESPONSE_DUPLICATED_CODE_VALUE = 1004 #在insert时表示有重复的错误码, RESPONSE_CODE_NAME 

    type_to_key_fields_str_dict = {
            #type
            65536: {
                'media_to_cur_field_map' : { 
                    #cgi的参数名=>本身的属性名
                    'test'          : 'type',
                    },
                'replace_merge_fields' : ['host'],
                'addition_merge_fields' : [],
                'use_backup_only_fields' : [],
                'primary_key_fields' : ['title', 'tv_station_name'],
                'default_value' : {
                    },
                },
            }

    type_to_key_fields_id_dict = {
            #type
            65536: {
                'media_to_cur_field_map' : {
                    #cgi的参数名=>本身的属性名
                    'test_id'       : 'media_response_retcode',
                    },
                'replace_merge_fields' : ['host'],
                'addition_merge_fields' : [],
                'use_backup_only_fields' : [],
                'primary_key_fields' : ['title', 'tv_station_id'],
                'default_value' : {
                    },
                },
            }

    TYPE_NAME_IN_MEDIA_CGI = None#和get_type()配对, type 
    type = 65536

    def __init__(self):
        self.media_response_retcode = -100#0成功, 是cgi返回错误码
        self.media_response_errmsg = "initial msg"
        self.__setattr__(self.MEDIA_ID_NAME, None)
        self.result_code = 0#0成功,是helper设置的错误值

    def get_type(self):
        return self.type 

    def get_id(self):
        return self.__getattribute__(self.PRIMARY_ID_NAME)

    def get_media_id(self):
        return self.__getattribute__(self.MEDIA_ID_NAME)

    def set_media_id(self, media_id):
        self.__setattr__(self.MEDIA_ID_NAME, media_id)

    #表示是否调用cgi成功并且解释的时候没有异常
    def set_media_update_fail(self):
        self.result_code = -1
    def reset_update_state(self):
        self.result_code = 0
    def is_call_cgi_sucess(self):
        if 0 == self.result_code:
            return True
        return False
    #记录cgi返回值的错误码, 有些cgi返回之后要根据错误码进行进一步处理
    def setcgi_ret_msg(self, ret, err_msg):
        self.media_response_retcode = ret
        self.media_response_errmsg =err_msg 
    def get_cgi_ret_code(self):
        """
        注意media_response_retcode如果是1024, 这是表示调用插入cgi的返回值，没有主键
        """
        return self.media_response_retcode, self.media_response_errmsg 

    def get_key_fields_dict(self, fields = []):
        if self.type not in self.type_to_key_fields_str_dict:
            raise Exception("type has no key field dict, type=%s" % self.type)
        logger.debug("fields = %s" % fields)
        key_fields_dict = {}
        # 需要把、；，,替换成|
        PUNC_SUB_RE = re.compile(u'[,\uff1b\u3001\uff0c;]')
        for media_name, self_name in self.type_to_key_fields_str_dict[self.type]['media_to_cur_field_map'].items():
            if fields and self_name not in fields:
                continue
            if not self.__getattribute__(self_name):
                key_fields_dict[media_name] = self.__getattribute__(self_name)
                continue
            if self_name in set(['srcsite', 'copyright']):
                if self.media_id and self.media_id != 'None':
                    continue

            if self_name:
                key_fields_dict[media_name] = self.__getattribute__(self_name)
            else:
                key_fields_dict[media_name] = ''
            # 若字典为0或空字符串都会被默认值覆盖，有覆盖有效值的风险
            if not key_fields_dict[media_name] and media_name in self.type_to_key_fields_str_dict[self.type]['default_value'].keys():
                key_fields_dict[media_name] = self.type_to_key_fields_str_dict[self.type]['default_value'][media_name]
            logger.debug("media_name=%s, is_enum=%s" % (media_name, media_name in self.get_media_enum_field_set()))
            if key_fields_dict[media_name] == '' and media_name in get_media_required_field_set(self.type):
                key_fields_dict[media_name] = "|"
            elif media_name in self.get_media_enum_field_set():
                try:
                    key_fields_dict[media_name] = PUNC_SUB_RE.sub("|", key_fields_dict[media_name])
                except:
                    logger.debug("replace delimiter fail, field_value=%s" % (key_fields_dict[media_name]))

        for media_name, self_name in self.type_to_key_fields_id_dict[self.type]['media_to_cur_field_map'].items():
            if fields and self_name not in fields:
                continue
            if not self.__getattribute__(self_name):
                continue
            if self_name:
                key_fields_dict[media_name] = self.__getattribute__(self_name)
            else:
                key_fields_dict[media_name] = ''
            # 若字典为0或空字符串都会被默认值覆盖，有覆盖有效值的风险
            if not key_fields_dict[media_name] and media_name in self.type_to_key_fields_id_dict[self.type]['default_value'].keys():
                key_fields_dict[media_name] = self.type_to_key_fields_id_dict[self.type]['default_value'][media_name]
            logger.debug("media_name=%s, is_enum=%s" % (media_name, media_name in self.get_media_enum_field_set()))
            if key_fields_dict[media_name] == '' and media_name in get_media_required_field_set(self.type):
                key_fields_dict[media_name] = "|"
            elif media_name in self.get_media_enum_field_set():
                try:
                    key_fields_dict[media_name] = PUNC_SUB_RE.sub("|", key_fields_dict[media_name])
                except:
                    logger.debug("replace delimiter fail, field_value=%s" % (key_fields_dict[media_name]))

        for k,v in key_fields_dict.items():
            if isinstance(v, str):
                v = v[0:20480]
        return key_fields_dict

#################################### 下面的函数可以继承覆盖实现, 上面的是基础函数，无需改动 #####################################
    #是否需要更新，调用更新函数时，本函数返回true才能进行更新操作 
    def could_update(self):
        return True
    
    #表示是可遍历的enum的属性
    def get_media_enum_field_set(self):
        return set()

    #调用插入函数时，需要的参数
    def get_insert_key_fields_dict(self):
        if not self.INSERT_KEY_FIELDS:
            logger.error("no params for insert cgi??")
            return {}
        return self.get_key_fields_dict(self.INSERT_KEY_FIELDS)
    #调用更新函数时，需要的参数
    def get_update_key_fields_dict(self):
        if not self.UPDATE_KEY_FIELDS:
            logger.error("no params for update cgi??")
            return {}
        return self.get_key_fields_dict(self.UPDATE_KEY_FIELDS)


class CommonHttpInterface(object):
    def __init__(self):
        self.http_HOST = None
        self.BEFORE_JSON_PART = "j("
        self.AFTER_JSON_PART = ")"
        pass

    def insert_objects(self, objects):
        """
        调用插入信息cgi
        """
        if len(objects):
            obj = objects[0]
            self.insert_objects_by_media_class(obj.__class__, objects)

    def update_objects(self, objects):
        """
        更新信息
        """
        if len(objects):
            limited_objects = []
            relation_update_objects = []
            for obj in objects:
                if obj.could_update():
                    limited_objects.append(obj)
                else:
                    relation_update_objects.append(obj)
            logger.debug("len(objects)=%s, len(limited_objects)=%s" % (len(objects), len(limited_objects)))
            self.update_objects_with_no_limit(limited_objects)

    def get_request_param(self, cls, cgi, params):
        if 1 == debug_flag:
            print "cgi=%s, params=\n%s\n" % (cgi, '\n' . join(["%s:%s" % (k, v) for k, v in params.items()]))
            logger.debug("cgi=%s, params=\n%s\n" % (cgi, '\n' . join(["%s:%s" % (k, v) for k, v in params.items()])))

        self.http_conf = g_conf.__getattribute__(cls.MEDIA_HTTP_CONF) 
        host, port = attemp_get_host_from_zk(self.http_conf['host'], self.http_conf['port'])
        url = "http://%s:%d%s?otype=json" % (host, port, cgi)
        app_info = self.http_conf['app_info']
        if app_info:
            url += "&%s" % app_info
        if cgi == "/api_create_column":
            url += "&from=10"
        if self.http_conf.get('http_host', ''):
            self.http_HOST = self.http_conf['http_host']
        else:
            self.http_HOST = host
        data = urllib.urlencode(params)
        logger.debug("post data:%s" % data)
        decoded_url = "%s&%s" % (url, urllib.unquote(data).decode('utf8'))
        logger.debug("decode url=%s" % decoded_url)
        logger.debug("url:%s" % url)
        return url, data, decoded_url

    def post_to_cgi(self, url, data, decoded_url):
        try:
            req = urllib2.Request(url)
            if self.http_HOST: 
                if 1 == debug_flag:
                    print "add HOST:%s" % self.http_HOST
                    logger.info("add HOST:%s" % self.http_HOST)
                req.add_header('Host', self.http_HOST)

            opener = urllib2.build_opener()
            response = opener.open(req, data, timeout=8)
            response_string = response.read()
            logger.debug("response_string:\n%s" % response_string)
            if 1 == debug_flag:
                print "\n--------------\nurl=%s\ndata=%s\ndecoded_url=%s\n response_string=%s\n---------------------\n" \
                        % (url, data, decoded_url, response_string)

            return response_string
        except urllib2.HTTPError, e:
            logger.error('(%s)http request error code - %s.' % (url.encode('utf-8'), e.code))
        except urllib2.URLError, e:
            logger.error('(%s)http request error reason - %s.' % (url.encode('utf-8'), e.reason))
        except Exception, e:
            logger.error('(%s)http request generic exception: %s.' % (url.encode('utf-8'), traceback.format_exc()))

        if 1 == debug_flag:
            print "\n--------------\nurl=%s\ndata=%s\ndecoded_url=%s\n---------------------\n" \
                    % (url, data, decoded_url)

        raise Exception("post to cgi fail, url:%s, data:%s", url, data)

    def chanformCode(self, params):
        dict = {}
        for k, v in params.items():
            if v and not isinstance(v, int):
                dict[k] = v.encode('utf-8')
            else:
                dict[k] = v
        return dict

    def get_insert_params(self, cls, obj):
        """最主要的转换参数的函数，把obj的属性和媒资库的cgi的参数建立关系"""
        params = obj.get_insert_key_fields_dict()
        if hasattr(cls, 'TYPE_NAME_IN_MEDIA_CGI') and cls.TYPE_NAME_IN_MEDIA_CGI:
            if cls.TYPE_NAME_IN_MEDIA_CGI in params.keys():
                raise Exception("media obj can't use %s as field name" % cls.TYPE_NAME_IN_MEDIA_CGI)
            params[cls.TYPE_NAME_IN_MEDIA_CGI] = obj.get_type()
        if 1 == debug_flag:
            print "\ninsert params = %s\n" % params 
        logger.info("insert params = %s" % params)
        dict = self.chanformCode(params)
        return dict

    def get_update_params(self, cls, obj):
        """最主要的转换参数的函数，把obj的属性和媒资库的cgi的参数建立关系"""
        params = obj.get_update_key_fields_dict()
        params[cls.MEDIA_ID_NAME] = obj.get_id()
        if 1 == debug_flag:
            print "\nupdate params = %s\n" % params 
        logger.info("update params = %s" % params) 

        dict = self.chanformCode(params)
        return dict

    def insert_objects_by_media_class(self, cls, objects):
        for obj in objects:
            decoded_url = ''
            try:
                logger.debug("new object:%s" % obj.__dict__)
                params = self.get_insert_params(cls, obj)
                url, data, decoded_url = self.get_request_param(cls, cls.INSERT_CGI_PATH, params)
                response_string = self.post_to_cgi(url, data, decoded_url)
                root_json = self.parse_json_from_response_string(response_string)
                self.check_response_json(cls, root_json, obj)
                if cls.MEDIA_ID_NAME_IN_RESPONSE: 
                    self.do_relative_task_after_insert(obj, self.parse_id_from_response_json(cls, root_json, obj))
                    obj.set_media_id(self.parse_id_from_response_json(cls, root_json, obj))
                else:
                    self.do_relative_task_after_insert(obj, None)

            except Exception, e:
                obj.set_media_update_fail()
                logger.error('%s' % traceback.format_exc())
                logger.error("insert object fail, obj=%s, decoded_url:%s", obj.__dict__, decoded_url)

    def update_objects_with_no_limit(self, objects): 
        if len(objects):
            obj = objects[0]
            self.update_objects_by_media_class(obj.__class__, objects)

    def update_objects_by_media_class(self, cls, objects):
        for obj in objects:
            decoded_url = ''
            try:
                params = self.get_update_params(cls, obj)
                url, data, decoded_url = self.get_request_param(cls, cls.UPDATE_CGI_PATH, params)
                response_string = self.post_to_cgi(url, data, decoded_url)
                root_json = self.parse_json_from_response_string(response_string)
                self.check_response_json(cls, root_json, obj)
                self.do_relative_task_after_update(cls, obj)
            except Exception:
                obj.set_media_update_fail()
                logger.error('%s' % traceback.format_exc())
                logger.error("update object fail, obj=%s, decoded_url:%s", obj.__dict__, decoded_url)


    def check_response_json(self, cls, root_json, obj):
        obj.setcgi_ret_msg(root_json[cls.RESPONSE_CODE_NAME], root_json[cls.RESPONSE_CODE_MSG]) 
        if root_json[cls.RESPONSE_CODE_NAME] != 0: 
            if cls.RESPONSE_DUPLICATED_CODE_VALUE: 
                if cls.RESPONSE_DUPLICATED_CODE_VALUE == root_json[cls.RESPONSE_CODE_NAME]: 
                    obj.setcgi_ret_msg(root_json[cls.RESPONSE_CODE_NAME], root_json[cls.RESPONSE_CODE_MSG]) 
                    return
            raise Exception("root_json:%s", json.dumps(root_json, ensure_ascii=False).encode('utf8'))

    def parse_id_from_response_json(self, cls, root_json, obj):
        if cls.MEDIA_ID_NAME_IN_RESPONSE:
            if cls.MEDIA_ID_NAME_IN_RESPONSE not in root_json.keys():
                obj.setcgi_ret_msg(1024, 'response without primary key:%s' % cls.MEDIA_ID_NAME_IN_RESPONSE ) 
                raise Exception("media_id(%s) not in root_json, root_json:%s"
                        % (cls.MEDIA_ID_NAME_IN_RESPONSE , json.dumps(root_json, ensure_ascii=False).encode('utf8')))
            return root_json[cls.MEDIA_ID_NAME_IN_RESPONSE]
        return None

    def parse_json_from_response_string(self, response_string):
        '''
        QZOutputJson={json_content};
        '''
        if not response_string.startswith(self.BEFORE_JSON_PART) or not response_string.endswith(self.AFTER_JSON_PART):
            raise Exception("unknown response string format")
        json_string = response_string[len(self.BEFORE_JSON_PART):(len(response_string) - len(self.AFTER_JSON_PART))]
        return json.loads(json_string)

    def do_relative_task_after_insert(self, obj, media_id):
        """
        可继承, 插入成功后会调用本函数
        """
        pass

    def do_relative_task_after_update(self, cls, obj):
        """
        可继承, 更新成功后会调用本函数
        """
        pass

class TestHttp(IHttpHelperInterface):
    def __init__(self):
        IHttpHelperInterface.__init__(self)


def test():
    test = TestHttp()
    httpHelper = CommonHttpInterface()
    httpHelper.insert_objects([test])
    httpHelper.update_objects([test])

def main():
    test()

if __name__ == "__main__":
    main()
