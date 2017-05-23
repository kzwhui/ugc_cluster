#!/usr/bin/python
#encoding=utf8
import re
import os 
import pickle
import sys
import getopt
import json
import time,datetime
import logging
import traceback
sys.path.append('../common')
sys.path.append('../conf')
reload(sys) 
sys.setdefaultencoding('utf-8')
import MySQLdb
from log import logger
from db import DBQueryPool
import common
sys.path.append('../common/redis.zip')
from redis_helper import CRedisHelper

# protobuf/
from es_conf import g_es_conf
sys.path.append('../es_sync/')
from api.utils import ShellHelper, get_id_md5
from api.protocol_to_json import Protocol2Json

sys.path.append('../pylib/protobuf.zip')
import search_basic_data_pb2
import pbjson

debug_flag = 0

def format_data_for_json(val, defaut_val = ''):
    if val is None:
        return defaut_val 
    if isinstance(val, int):
        return val
    if isinstance(val, list):
        formated_list = []
        for e in val:
            formated_list.append(format_data_for_json(e, defaut_val))

        return formated_list
    new_val = val.replace('\t', '')
    new_val = val.replace('\n', '')
    #new_val = val.replace(' ', '')
    return new_val 

#获取哪些分类是精品、综合的配置
def get_quality_config():
    mapTypeToConfig = {}#这个dict中存在key的type都是精品的
    sql = "select c_entity_id,c_url, c_special_rules from t_quality_content where c_entity_id is not null and  "\
            "c_url is not null and c_valid = 1 and c_config_type in('cover_type') and c_entity_type in('cover')"
    
    rows = DBQueryPool.get_instance("Search").get_dict(sql)
    for r in rows:
        type = int(r['c_entity_id'])
        config = r['c_url']
        if config:
            mapTypeToConfig[type] = config 
            mapTypeToConfig["%s" % type] = config 
        else:
            mapTypeToConfig[type] = ""
            mapTypeToConfig["%s" % type] = ""

    return mapTypeToConfig

#获取web侧tabid信息
def get_tabid_map():
    mapTabidInfo = {}
    sql = "SELECT c_idx,c_desc FROM t_tpl_zt_msg where c_tpl_id=600 and c_zt_id=3 and c_block_id=1 and c_valid=1 order by c_pos"
    rows = DBQueryPool.get_instance("d_web_cfg").get_dict(sql)
    for r in rows:
        tabid = r['c_idx']
        old_type_id_str = r['c_desc']
        old_type_id_list = old_type_id_str.split("+")
        for type_id in old_type_id_list: 
            mapTabidInfo["%s" % type_id] = "%s" % tabid 

    return mapTabidInfo


#取WA、WB、WC信息
def get_score_from_db(id_list):
    if not id_list:
        return {}
    sql = "select c_cid, c_search_score, c_latest_score, c_hottest_score from t_rank_cover_result "\
            "where c_search_score is not null and c_latest_score is not null and c_hottest_score is not null and c_cid in (%s)" \
            % (',' . join(["'%s'" % id for id in id_list]))
    rows = DBQueryPool.get_instance("d_data_center").get_dict(sql)
    mapIdToScores = {}
    for r in rows:
        id = r['c_cid']
        sortScore = r['c_search_score']
        latestScore = r['c_latest_score']
        hottestScore = r['c_hottest_score']
        o = CSosoProtocolV3()
        o.__setattr__("WA", sortScore)
        o.__setattr__("WB", latestScore)
        o.__setattr__("WC", hottestScore)
        mapIdToScores[id] = o

    return mapIdToScores


class CSosoProtocolV3(object):
    KYES = ['ID', 'IB', 'IA', 'MD', 'IE', 'IF', 'IG', 'IH', 'IJ', 'IC', 'TA', 'TB', 'TC', 'TD', 'TE', 'TF', \
            'VA', 'VB', 'VC', 'VD', 'VE', 'VF', 'VG', 'PA', 'PB', 'PC', 'PD', 'PE', 'YA', 'YB', \
            'YC', 'YD', 'YE', 'YF', 'YG', 'YH', 'YI', 'RA', 'RB', 'RC', 'RD', 'RE', 'RF', 'RG', \
            'RH', 'RI', 'UA', 'UB', 'DA', 'DB', 'DC', 'DD', 'DU', 'DG', 'DH', 'DK', 'DL', 'DM', 'DN', 'WA', 'WB', 'WC', 'WD', 'WE', 'WF', 'IN', 'SA', \
            'SB', 'SC', 'SD', 'SE', 'SF', 'AA', 'AB', 'AC', 'PL', 'CA', 'CB', 'CC', 'X1', 'PF', \
            'FA', 'FB', 'FC', 'FD', 'FE', 'FF', 'FG', 'FH', 'FI', 'II', 'IJ', 'SI', 'DE', 'DF', 'PI', \
            'SG', 'TH', 'TG', 'PN', 'PM', 'PJ', 'PK', 'FK', 'FJ', 'XF', 'IL', 'IC', 'IK', 'ZZMTIME'] #在这个list里面的的Key才会输出到文件，而且是按照这个list的顺序输出

    DEFAULT_VALUES = {'IA':'', 'IB' : '', 'ID' : '', 'MD' : ''}
    INTERGE_TYPE_KEYS = set(['YI', 'YH', 'WA', 'WB', 'WC', 'WD', 'SA', 'SB', 'SC', 'SD', 'SE', 'SF', 'SG', 'FE', 'FK', 'IF', 'IE', 'IG', 'VB', 'IH', 'UB', 'IL'])#整数的类型
    MUTI_INTERGE_TYPE_MERGED_KEYS = set(['PI', 'FA', 'FB', 'FC', 'FD', 'FH', 'FF', 'FG', 'FI'])#多个整数合并成的字段
    LARGER_THAN_ZERO = set(['YH', 'FE', 'IF', 'IE', 'IG', 'PI', 'FA', 'FB', 'FC', 'FD', 'FH'])#值域大于0的

    def __init__(self):
        for k in self.KYES:
            if self.DEFAULT_VALUES.get(k, None): 
                self.__setattr__(k,self.DEFAULT_VALUES.get(k, None))
            elif k in self.INTERGE_TYPE_KEYS and k not in self.LARGER_THAN_ZERO: 
                self.__setattr__(k, 0)
            else:
                self.__setattr__(k, None)

    def get_id(self):
        if 1 == int(self.IF):
            return self.ID
        if 2 == int(self.IF):
            return self.IB
        if 3 == int(self.IF):
            return self.IA
        if 23 == int(self.IF):
            return self.ID
        if 14 == int(self.IF): # 专题
            return self.ID
        return None

    def generate_search_weight_SG(self):
        """
        SG字段合成，专门给综合区排序使用
        SG为0为不调整权重，大于0为提权，小于0为降权
        需求简单描述:
            精品区结果在展现时是包括剧集列表的，希望综合区里能根据视频业务特点对已经出现在精品区里的相关结果进行过滤或排序优化。
            例如：电视剧，电影，动漫的综合区希望优先排序编辑运营的非正片的单视频内容；综艺的综合区优先排序非完整版正片的单视频；时尚和生活优先列出所有栏目相关专辑。
            时间计划：a. 业务离线数据提供编辑视频标识字段（可能需增加字段），10.26~10.28；b. 引擎L4相关性调整，10.29~11.3。
        """
        if 0 != self.IG:
            return 0
        if self.IE not in set([2,4,6]):
            return 0
        if self.FE > 0 and self.FE <= 3:
            #电影电视剧,动漫
            if 1 == self.IF:
                #单视频
                if self.IB and self.YE and self.YI in set([1,3]):
                    return -1

        elif 10 == self.FE:
            #综艺
            if self.IA and self.YE:
                is_positive = False
                if '正片' == self.YE:
                    is_positive = True
                if 1 == self.IF:
                    if is_positive and self.TF and self.TF.find("完整版") >= 0:
                        return -2

                elif 2 == self.IF:
                    if is_positive:
                        return -3
                    return -1

        elif self.FE in set([25, 31]):
            if self.IA:
                return 1

        return 0

    def format_data(self):
        if self.DA is not None and (self.DA == 0 or self.DA == '0'):
            self.DA = ''

        for k in self.KYES: 
            if hasattr(self, k) and self.__getattribute__(k) is not None \
               and not isinstance(self.__getattribute__(k), int):
                if self.__getattribute__(k) == '不详' or self.__getattribute__(k) == '暂无':
                    self.__setattr__(k, '')

        if k in self.INTERGE_TYPE_KEYS: 
            self.transform_to_int(k)

        for k in self.MUTI_INTERGE_TYPE_MERGED_KEYS:
            if hasattr(self, k):
                v = self.__getattribute__(k)
                if v is not None and not isinstance(v, int):
                    value_set = set()
                    value_array = v.split(";")
                    for item in value_array: 
                        if item is None or item == '':
                            logger.debug("ignore empty value for k:[%s] [%s] [%s]" % (self.get_id(), k, v))
                            continue
                        int_item = CSosoProtocolV3.to_int(item)
                        if int_item != 0:
                            value_set.add(int_item) 
                            continue
                        if k in self.LARGER_THAN_ZERO:
                            logger.debug("ignore 0 value for k:[%s] [%s] [%s]" % (self.get_id(), k, v))
                            continue
                        value_set.add(int_item)

                    if value_set:
                        v = self.__setattr__(k, ";" . join(["%s" % item for item in value_set]))
                    else:
                        v = self.__setattr__(k, '')


    def process(self, mapIdToScores):
        if mapIdToScores:
            if self.ID or self.IB:
                weightO = mapIdToScores.get(self.ID if self.ID else self.IB, None)
                if weightO:
                    self.WA = weightO.WA
                    self.WB = weightO.WB
                    self.WC = weightO.WC

        self.__setattr__('SG', self.generate_search_weight_SG())
        #DG(c_first_publish_time): 时间日期无需索引首期正片/预告片上线时间（优先正片，有其他字段标记是否是正片）/视频首次上架时间
        #DH(c_latest_publish_time): 时间日期无需索引最新期正片上线时间/视频更新时间
        self.__setattr__('DG', self.DB)
        self.__setattr__('DH', self.ZZMTIME)
        #mingyangan(安明洋) 10-19 09:58:02
        #http://tapd.oa.com/tvideo/prong/stories/view/1010031991057490637
        #您好，这个需求和您确认下，从NBA页面跳转搜索时，是只需要出这个分类下运营的数据是么？
        #surizhou(周姝) 10-19 10:06:23
        #是的

        #mingyangan最终确认的：栏目、专辑、视频都要，要限制三个分类
        if self.YE == 'NBA' and self.YB == '精彩篮球' and self.YA == '体育':
            if self.FA:
                self.FA = "%s;%s" % (self.FA, '6')
            else:
                self.FA = "6"

        #if 1 == debug_flag:
        #    keys_of_pt = sorted(self.KYES)
        #    for k in keys_of_pt:#self.KYES: 
        #        if hasattr(self, k) and self.__getattribute__(k) is not None:
        #            print "%s:%s" % (k, self.__getattribute__(k))

        self.format_data()

        if 1 == debug_flag:
            print "\n############################################################\n"
            keys_of_pt = sorted(self.KYES)
            for k in keys_of_pt:#self.KYES: 
                if hasattr(self, k) and self.__getattribute__(k) is not None:
                    print "%s:%s" % (k, self.__getattribute__(k))

    def print_debug_info(self):
        keys_of_pt = sorted(self.KYES)
        for k in keys_of_pt:#self.KYES: 
            if hasattr(self, k) and self.__getattribute__(k) is not None:
                logger.debug( "%s:%s" % (k, self.__getattribute__(k)))


    def transform_to_int(self, attr_name):
        self.__setattr__(attr_name, CSosoProtocolV3.to_int(self.__getattribute__(attr_name)))
        v = self.__getattribute__(attr_name)
        if v is not None and v == 0 and attr_name in self.LARGER_THAN_ZERO:
            self.__setattr__(attr_name, None)

    @staticmethod
    def to_int(str):
        if str is None:
            return 0
        try:
            return int(str)
        except Exception,e:
            logger.error(traceback.format_exc())
            logger.error("to int err:[%s] " % str)
            return 0
        return str

    def from_dict(self, dict, is_gbk = True, ignore_zero_value = False):
        if not dict:
            logger.error("empty dict")
            return
        for k, v in dict.items():
            if v:
                uv = None
                if is_gbk and not isinstance(v, int): 
                    try:
                        uv = v.decode('gbk')
                        if k == 'CB':
                            jsDecode = json.loads(uv)
                            if 'playright' in jsDecode.keys():
                                jsDecode['playright'] = ''

                            uv = json.dumps(jsDecode)

                    except Exception,e:
                        logger.error("error line:[%s] [%s] [%s] " % (k, v, dict["MD"]))
                        logger.error(traceback.format_exc())
                        continue
                else:
                    uv = v
                if v == '0' and ignore_zero_value:
                    continue
                self.__setattr__(k, uv)
            else:
                self.__setattr__(k, v)

        self.format_data()

class COutputHelper(object):
    #过滤精品的数据
    def _filter_medias(self, media_dict):
        if not media_dict:
            return media_dict
        filter_dict = {}
        for id, o in media_dict.items():
            if hasattr(o, 'IG'):
                if o.IG != 0 and o.IG != '0':
                    logger.info("filter id:[%s] [%s]" % (o.get_id(), o.IG))
                    continue
            filter_dict[id] = o

        return filter_dict

    #对外接口
    def add_medias_to_storage(self, media_dict, store_complex_data = True):
        if store_complex_data:
            media_dict = self._filter_medias(media_dict)

        return self._add_medias_to_storage(media_dict)
    #必须继承
    def _add_medias_to_storage(self, media_dict):
        pass

    #对外接口
    def finish_added(self):
        pass


class COutputComplexFile(COutputHelper):
    def __init__(self, output_path, curr_stamp=None):
        self.timestamp = int(time.time())
        if curr_stamp:
            self.timestamp = int(curr_stamp)
        
        self.output_path = output_path
        self.file_name = '%s/new.%s.new' % (output_path, self.timestamp) 
        self.file_prefix = "%s/new.%s" % (output_path, self.timestamp)
        self.file_suffix = "new"

    def _add_medias_to_storage(self, media_dict):
        return self.add_medias_to_file(media_dict)

    def finish_added(self):
        return self.final_write_and_integrity_check()

    def add_medias_to_file(self, media_dict):
        if not media_dict:
            return False
        try:
            outfile = open(self.file_name, "a")
            if os.path.getsize(self.file_name)>0:
                outfile.write("\n")

            for id, o in media_dict.items():
                outfile.write("!!\n")
                for field in o.KYES:
                    value = ""
                    if o.__getattribute__(field) is not None:
                        value = "%s" % o.__getattribute__(field)

                    if value and field == 'ZZMTIME':
                        value = int(time.mktime(time.strptime(value, "%Y-%m-%d %H:%M:%S")))
                    else:
                        value = value.replace("\n", "").replace("\r", "")

                    outfile.write(field)
                    if value:
                        try:
                            if isinstance(value, int):
                                outfile.write("%s" % value)
                            else:
                                outfile.write(value.decode("utf8").encode("gbk"))
                        except:
                            logger.error("encode to gbk fail, field=%s, value=%s" % (field, value))
                            outfile.write("")

                    outfile.write("\n")

                outfile.write("\n")
                logger.info("%s write:%s." % (common.log_str(id), self.file_name))

        except Exception, e:
            logger.error(traceback.format_exc())
            raise Exception("write file Exception")

    def final_write_and_integrity_check(self):
        if not os.path.isfile(self.file_name):
            logger.error("no such file:[%s]" % self.file_name)
            return ()
        if os.path.getsize(self.file_name) <= 0:
            logger.info("empty file:[%s]" % self.file_name)
            return ()
        md5_file_name = "%s.md5" % (self.file_prefix)
        cmd = "md5sum  %s > %s" % (self.file_name, md5_file_name) 
        logger.info("%s" % cmd)
        os.system(cmd)
        sign_file_name = "%s.sign" % (self.file_prefix)
        cmd = "echo '%s' > %s" % (self.timestamp, sign_file_name) 
        logger.info("%s" % cmd)
        os.system(cmd)
        return (self.file_name, md5_file_name, sign_file_name)

class COutputDeleteFile(COutputComplexFile):
    def __init__(self, output_path, curr_stamp=None):
        COutputComplexFile.__init__(self, output_path, curr_stamp)
        self.file_name = '%s/%s.mv' % (output_path, self.timestamp) 
        self.file_prefix = "%s/%s" % (output_path, self.timestamp) 
        self.file_suffix = "mv"

    def add_medias_to_file(self, media_dict):
        if not media_dict:
            return False
        try:
            outfile = open(self.file_name, "a")
            for id, o in media_dict.items():
                outfile.write("%s\n" % o.MD)

        except Exception, e:
            logger.error(traceback.format_exc())
            raise Exception("write file Exception")

class CShellHelper(object):

    def run_cmd(self, command):
        """Run command and return (out,err)"""
        import subprocess
        if type(command) == str:
            command = command.split(" ")
        p = subprocess.Popen(command, stdout=subprocess.PIPE)
        out, err = p.communicate()
        self.last_err = err
        return out

    def mysql_exec(self, sql, mysql_shell="mysql"):
        mysql_command = mysql_shell.split(' ')
        mysql_command.extend(['-s', '-e', sql])
        return self.run_cmd(mysql_command).split('\n')

    def attr_api(self, attr_id, value):
        cmd = "/usr/local/agenttools/agent/agentRepNum %s %s" % (attr_id, value)
        return self.run_cmd(cmd)

class CRedisWriter(CRedisHelper):
    """
    将指定数据写入Redis：
      r = CRedisWriter(addr={'ip': '10.123.9.22', 'port': 6379})
      r = CRedisWriter(zkname='vimix_test.redis.com')
    """

    def __init__(self, addr=None, zkname=None, key_ttl=0):
        """
        指定zkname或addr连接Redis，不指定则使用 g_conf.REDIS_CONF 配置
          addr: {'ip': '10.123.9.22', 'port': 6379}
          zkname: 'vimix_test.redis.com'
        """
        if addr or zkname:
            self.config = { 'addrs': addr, 'zk_name': zkname }
            self.r = super(CRedisWriter,self).__init__(self.config)
        else:
            self.r = super(CRedisWriter,self).__init__() # init with default
        self.key_ttl = key_ttl

        # 获取要同步的字段
        self.sync_fields = self._get_sync_fields()

    def _get_sync_fields(self):
        """
        目前同步的字段只有1个DB有，先硬编码获取过程
        """
        mysql_shell = "mysql -h10.240.64.138 -P3640 -ud_search_info -pae9baecd9 --default-character-set=utf8 d_complex_data_def"
        sql = "SELECT c_prefix AS prefix, GROUP_CONCAT(c_sync_field SEPARATOR ',') AS fields from t_data_sync_redis_key_fields_def GROUP BY c_prefix;"
 
        result = {}
        for line in CShellHelper().mysql_exec(mysql_shell=mysql_shell, sql=sql):
            if len(line) < 3: continue
            data = line.split()
            result[data[0]] = sorted(data[-1].split(","))
        return result

    def add_medias_to_redis(self, media_dict, trace=False):
        if not media_dict:
            return False
        try:
            key_list = []
            for u, o in media_dict.items():
                # 获取要同步的字段和fields
                for prefix, fields in self.sync_fields.items():
                    key = "%s%s" % (prefix, o.MD)

                    json_record = {}
                    for f in fields:
                        if hasattr(o, f):
                            val = o.__getattribute__(f)
                            if trace: print f, type(val)
                            if not val in [None, '']:    # 去掉空字符串和int的0，注意：还有字符串的'0'
                                # 为了确保sort长度，TC仅在需要用到未删减时才同步
                                if trace: print "found %s" % f
                                if (prefix == 'sort_'):
                                    if (f == 'TC') and (val.find('未删减') == -1):
                                        continue
                                    elif (f == 'DB'):
                                        # DB -> DB_ts
                                        try:
                                            DB = val
                                            if type(val) in [str, unicode]:
                                                DB = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                                            if type(DB) == datetime.datetime:
                                                json_record['DB_ts'] = int(time.mktime(DB.timetuple()))
                                        except Exception, e:
                                            logger.debug("field(%s) = %s process failed: %s" % (f, val, e))
                                            continue
                                    elif (f in ['DG', 'DH', 'DI', 'DJ', 'DK', 'DL']):
                                        # sort中的时间字段，只保留需要的时间戳
                                        try:
                                            sort_datetime = val
                                            if type(val) in [str, unicode]:
                                                sort_datetime = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
                                            if type(sort_datetime) == datetime.datetime:
                                                # set only when datetime
                                                json_record[f] = int(time.mktime(sort_datetime.timetuple()))
                                                continue
                                            raise Exception("invalid %s = %s" % (f, sort_datetime))
                                        except Exception, e:
                                            # print("field(%s) = %s process failed: %s" % (f, val.encode('utf8'), e))
                                            logger.debug("field(%s) = %s process failed: %s" % (f, val.encode('utf8'), e))
                                            continue

                                if (f in ['IJ']): # replace + to ;
                                    IJ_list = [ i for i in val.split('+') if len(i) > 0 ]
                                    val = ';'.join(IJ_list)

                                if type(val) == datetime.datetime:
                                    val = val.strftime("%Y-%m-%d %H:%M:%S")
                                elif f in CSosoProtocolV3().INTERGE_TYPE_KEYS:
                                    val = self.to_int(f, val)

                                json_record[f] = val

                    # 注意这里的性能
                    json_str = json.dumps(json_record)

                    # 将 key, json_str 写入redis
                    if self.key_ttl > 0:
                        self.set_value(key, json_str, self.key_ttl)
                    else:
                        self.set_value(key, json_str)
                    key_list.append(key)
                
                    # 对栏目(IF=3)，需要额外写一份 md5(column_<ID>) 的数据到Redis
                    if o.IF in [3, '3', u'3']:
                        column_id = o.IA.split("_")[1]
                        column_new_md = get_id_md5("column_" + column_id)
                        key = "%s%s" % (prefix, column_new_md)
                        json_record['MD'] = column_new_md
                        json_str = json.dumps(json_record)

                        if self.key_ttl > 0:
                            self.set_value(key, json_str, self.key_ttl)
                        else:
                            self.set_value(key, json_str)
                        key_list.append(key)

            return key_list

        except Exception, e:
            logger.error(traceback.format_exc())
            raise Exception("write redis Exception: %s" % e)

    def to_int(self, name, val):
        try:
            return int(val)
        except:
            return val

    def final_write_and_flush_redis(self):
        """do nothing"""
        pass

class CRedisWriter2(CRedisHelper):
    """
    将指定数据写入Redis：
      r = CRedisWriter(addr={'ip': '10.123.9.22', 'port': 6379})
      r = CRedisWriter(zkname='vimix_test.redis.com')
    """

    def __init__(self, addr=None, zkname=None, key_ttl=0):
        """
        指定zkname或addr连接Redis，不指定则使用 g_conf.REDIS_CONF 配置
          addr: {'ip': '10.123.9.22', 'port': 6379}
          zkname: 'vimix_test.redis.com'
        """
        if addr or zkname:
            self.config = { 'addrs': addr, 'zk_name': zkname }
            self.r = super(CRedisWriter2,self).__init__(self.config)
        else:
            self.r = super(CRedisWriter2,self).__init__() # init with default
        self.key_ttl = key_ttl

        self.shell = CShellHelper()

    def decode(self, key):
        value = self.get_value(key)
        pb_object = search_basic_data_pb2.CMediaInfo()
        pb_object.ParseFromString(value)
        return pb_object

    def decode_to_json(self, key):
        return pbjson.pb2json(self.decode(key))

    def logstash_info(self, obj, res, prefix):
        if obj.IG == 1:
            source_type = 'quality'
        else:
            source_type = 'complex'
            
        if obj.IF in ['', None]:
            IF = None
        else:
            IF = int(obj.IF)
        
        if IF in [1,9,17,23, 14]:
            title = obj.TF
        elif IF in [2,16, 5]:
            title = obj.TC
        else:
            title = obj.TA
        
        info = "%sREDIS|add|%s|%s|%s|%s|%s|%s|%s|%s|%s|" % (prefix, source_type, str(obj.FE), str(obj.YA), res, str(obj.FA), str(obj.IF), str(obj.FH), str(obj.FD), str(title))
        return info
        
    def set_pb_to_redis(self, media_dict, trace=False, attr_id=0):
        if not media_dict:
            return False
        try:
            # use pipeline for batch setex
            pipe = self.get_pipeline()
            key_list = []
            p2j = Protocol2Json(g_es_conf.DB_CONFS['t_data_sync_redis_key_fields_def'])
            for k,obj in media_dict.items():
                for prefix in ['sort_', 'static_']:
                    key = "%s%s" % (prefix, obj.MD)
                    json_record = p2j.to_json(obj, prefix, trace)
                    # if trace: print json_record
                    try:
                        pb_object = pbjson.json2pb(search_basic_data_pb2.CMediaInfo, json.dumps(json_record), True)
                    except Exception, e:
                        logger.error(traceback.format_exc())
                        logger.error("PB> ID=%s: %s\n%s" % (obj.get_id(), e, json.dumps(json_record)))
                        logger.info("%s%s json2pb error." % (common.log_str(obj.get_id()), self.logstash_info(obj, 'fail', prefix)))
                        print("[ERROR] protobuf ID=%s: %s\n%s" % (obj.get_id(), e, json.dumps(json_record)))
                        continue

                    if self.key_ttl > 0:
                        self.pipe_set_value(pipe, key, pb_object.SerializeToString(), self.key_ttl)
                    else:
                        self.pipe_set_value(pipe, key, pb_object.SerializeToString())
                    logger.info("%s%s succ." % (common.log_str(obj.get_id()), self.logstash_info(obj, 'suc', prefix)))
                    key_list.append(key)

                    # 栏目还要同步一份 column_ 前缀的
                    if obj.IF in [3, '3', u'3']:
                        column_id = obj.IA.split("_")[1]
                        column_new_md = str(get_id_md5("column_" + column_id))
                        key = "%s%s" % (prefix, column_new_md)
                        json_record['id_mdsum'] = column_new_md
                        pb_object = pbjson.json2pb(search_basic_data_pb2.CMediaInfo, json.dumps(json_record), True)

                        if self.key_ttl > 0:
                            self.pipe_set_value(pipe, key, pb_object.SerializeToString(), self.key_ttl)
                        else:
                            self.pipe_set_value(pipe, key, pb_object.SerializeToString())
                        key_list.append(key)

            # execute and count successed oper
            results = pipe.execute()
            success = sum(s == True for s in results)
            self.shell.attr_api(attr_id, success)

            failed = len(key_list) - success
            if failed > 0:
                logger.error("[ERROR] set_pb_to_redis() failed %d/%d keys" % (failed, len(key_list)))
                print("[ERROR] set_pb_to_redis() failed %d/%d keys" % (failed, len(key_list)))

            return key_list

        except Exception, e:
            logger.error(traceback.format_exc())
            raise Exception("write redis Exception: %s" % (e))

    def final_write_and_flush_redis(self):
        """do nothing"""
        pass


def _get_table_name_from_id(ID, gtype=1):
    if not ID:
        logger.error("no ID?")
        return None
    if gtype == 5:
        db_prefix = "d_search_ugc_"
    else:
        db_prefix = "d_complex_db_static_"
    offset = common.rs_hash("%s" % ID) % 100
    db_name = "%s%s" % (db_prefix, offset) 
    offset = common.FNVHash1("%s" % ID) % 100
    if gtype == 5:
        table_name = 't_search_ugc_%s' % offset
    else:
        table_name = 't_direct_static_%s' % offset
    real_table = "%s.%s" % (db_name, table_name)
    if 1 == debug_flag:
        logger.debug("ID=%s, real_table = %s " % (ID, real_table))
        print("ID=%s, real_table = %s " % (ID, real_table))

    return real_table 

def _get_kb_table_name_from_id(ID):
    if not ID:
        logger.error("no ID?")
        return None

    db_prefix = "d_kuaibao_data_" 
    offset = common.rs_hash("%s" % ID) % 100
    db_name = "%s%s" % (db_prefix, offset)
    offset = common.FNVHash1("%s" % ID) % 100
    table_name = 't_kuaibao_data_%s' % offset

    real_table = "%s.%s" % (db_name, table_name)
    if 1 == debug_flag:
        logger.debug("ID=%s, real_table = %s " % (ID, real_table))
        print("ID=%s, real_table = %s " % (ID, real_table))

    return real_table
    
def _get_fields(gtype=1, limited_fields = False, table_name='t_direct_static'):
    cur_keys_set = set(CSosoProtocolV3.KYES)
    db_fields_name_list = []
    db_fields_name_to_protocol_name = {}
    #db_fields_name_list = ["c_first_publish_time","c_latest_publish_time"]
    #db_fields_name_to_protocol_name = {"c_first_publish_time":"DG", "c_latest_publish_time":"DH"}
    if gtype == 5:
        sql = "select * from t_data_sync_field_def where c_table_name in('%s','t_xugc')" % (table_name)
    else:
        sql = "select * from t_data_sync_field_def where c_table_name = '%s'" % (table_name)
    if (gtype == 1 or gtype == 5):
        rows = DBQueryPool.get_instance("d_complex_data").get_dict(sql)
    else:
        rows = DBQueryPool.get_instance("d_search_sync").get_dict(sql)
    all_fields_set = set()
    for r in rows:
        protocol_name = r['c_sync_field']
        # skip quality fields for gtype=5
        if gtype == 5 and protocol_name in ['WE', 'WF', 'DM', 'DN']:
            continue
        all_fields_set.add( protocol_name )
        db_field_name = r['c_sync_name']
        if protocol_name in cur_keys_set or False == limited_fields: 
            db_fields_name_list.append(db_field_name)
            db_fields_name_to_protocol_name[db_field_name] = protocol_name 

    return db_fields_name_list,db_fields_name_to_protocol_name 

def join_one_insert_line_values(db_fields_name_list, db_fields_name_to_protocol_name, o):
    insert_values = ''
    available_value = 0
    for field_name in db_fields_name_list:
        name = db_fields_name_to_protocol_name[field_name]
        if insert_values:
            insert_values += ","

        if not hasattr(o, name) or o.__getattribute__(name) is None:
            insert_values += "''"
            continue
        if isinstance(o.__getattribute__(name), int):
            insert_values += "%d" % o.__getattribute__(name)
            available_value= available_value+1
            continue

        insert_values += "'%s'" % MySQLdb.escape_string ("%s" % o.__getattribute__(name))
        available_value= available_value+1

    if not available_value:
        logger.error(traceback.format_exc())
        raise Exception("invalid sql value")
    return insert_values 

def join_one_update_line_values(db_fields_name_list, db_fields_name_to_protocol_name, o):
    update_values = ''
    available_value = 0
    c_entity_type = 'video'
    if 2 == o.IF:
        c_entity_type = 'cover'
    elif 3 == o.IF:
        c_entity_type = 'column'

    for field_name in db_fields_name_list:
        name = db_fields_name_to_protocol_name[field_name]
        if update_values:
            update_values += ","

        if not hasattr(o, name) or o.__getattribute__(name) is None:
            update_values += "%s=''" % field_name
            continue
        if isinstance(o.__getattribute__(name), int):
            update_values += "%s=%d" % (field_name, o.__getattribute__(name))
            available_value= available_value+1
            continue

        update_values += "%s='%s'" % (field_name, MySQLdb.escape_string ("%s" % o.__getattribute__(name)))
        available_value= available_value+1

    if not available_value:
        logger.error(traceback.format_exc())
        raise Exception("invalid sql value")
    update_values += ",c_entity_type='%s'" % c_entity_type
    update_values += ",c_is_shield=0,c_is_valid=1" 
    return update_values 


def _do_sync_to_db(table_name, db_fields_name_list, db_fields_name_to_protocol_name, o_list, db_key='d_complex_data'):
    if not table_name or not o_list or not db_fields_name_list or not db_fields_name_to_protocol_name:
        logger.error(traceback.format_exc())
        raise Exception("parameters invalid")
    db_fields_name_without_MD_list = set(db_fields_name_list) - set(["c_id_md5"])
    for o in o_list:
        c_entity_type = 'video'
        if 2 == o.IF:
            c_entity_type = 'cover'
        elif 3 == o.IF:
            c_entity_type = 'column'

        insert_values = join_one_insert_line_values(db_fields_name_list, db_fields_name_to_protocol_name, o)
        update_values = join_one_update_line_values(db_fields_name_without_MD_list, db_fields_name_to_protocol_name, o)
        sql = "insert into %s (c_media_id, c_entity_type, c_create_time, %s) values ('%s', '%s', now(), %s) on duplicate key update c_media_id='%s', %s" %\
                (table_name, "," . join(["%s" % name for name in db_fields_name_list]),\
                 o.get_id(), c_entity_type, insert_values, o.get_id(), update_values)
        afects = DBQueryPool.get_instance(db_key).execute_sql(sql)


def write_to_db_batch(media_obj_dict):
    if not media_obj_dict:
        return True
    db_fields_name_list, db_fields_name_to_protocol_name = _get_fields(1, True)
    table_name_to_objs_list = {}
    failed_id_list = []
    for md, o in media_obj_dict.items():
        table_name = _get_table_name_from_id(o.get_id())
        if not table_name:
            logger.error("no table_name for md:[%s] " % md)
            continue

        if table_name_to_objs_list.get(table_name, None):
            table_name_to_objs_list[table_name].add(o)
        else:
            table_name_to_objs_list[table_name] = set([o])

    for table_name, o_list in table_name_to_objs_list.items():
        _do_sync_to_db(table_name, db_fields_name_list, db_fields_name_to_protocol_name, o_list)

    return True


def write_to_dynamic(media_obj_dict):
    if not media_obj_dict:
        return True
    db_fields_name_list, db_fields_name_to_protocol_name = _get_fields(0, True, 't_direct_dynamic')
    # 暂时只导入播放量
    db_fields_name_list = ['c_id_md5', 'c_total_play_count', 'c_week_play_count']
    db_fields_name_to_protocol_name['c_id_md5'] = 'MD'
    _do_sync_to_db('t_direct_dynamic', db_fields_name_list, db_fields_name_to_protocol_name,
            o_list=media_obj_dict.values(), db_key='d_search_sync')

    return True


def _load_ids_from_db(db_fields_name_to_protocol_name, table_name, part_id_set, gtype=1, include_all=True):
    if not db_fields_name_to_protocol_name or not table_name or not part_id_set:
        logger.error("invalid prameters")
        return []
    sql = "select %s from %s where c_media_id in (%s)" % \
            (',' . join(db_fields_name_to_protocol_name.keys()), \
             table_name, "," . join(["'%s'" % id for id in part_id_set]))
    if (gtype == 1):
        if not include_all:
            sql += ' and c_is_quality=0 and c_is_valid = 1 and c_is_shield = 0'
        rows = DBQueryPool.get_instance("d_complex_data").get_dict(sql)
    elif (gtype == 5):
        if not include_all:
            sql += ' and c_is_quality=0 and c_is_valid = 1 and c_is_shield = 0'
        rows = DBQueryPool.get_instance("d_xugc_data").get_dict(sql)
    else:
        if not include_all:
            sql += ' and c_is_quality=1 and c_is_valid = 1 and c_is_shield = 0'
        rows = DBQueryPool.get_instance("d_search_sync").get_dict(sql)
    if not rows:
        ids = ',' . join(part_id_set)
        logger.error("%s no such data in db:[%s]" % (common.log_str(ids), ids))
        return []
    objs_list = []
    for r in rows: 
        o = CSosoProtocolV3()
        for db_fields_name, protocol_name in db_fields_name_to_protocol_name.items():
            value = r.get(db_fields_name, None)
            o.__setattr__(protocol_name, None)
            if value is not None: 
                o.__setattr__(protocol_name, value)

        if o.get_id():
            objs_list.append(o) 
        else:
            logger.error("no id from db?")

    return objs_list

def read_from_db_batch(id_list, gtype=1, include_all=True):
    if not id_list:
        return {}
    table_name_to_objs_list = {}
    db_fields_name_list, db_fields_name_to_protocol_name = _get_fields(gtype)
    for id in id_list:
        if gtype in [1, 5]:
            table_name = _get_table_name_from_id(id, gtype)
        else:
            table_name = "t_direct_static"
        if not table_name:
            logger.error("no table_name for id:[%s] " % id) 
            continue

        if table_name_to_objs_list.get(table_name, None):
            table_name_to_objs_list[table_name].add(id)
        else:
            table_name_to_objs_list[table_name] = set([id])

    objs_dict = {}
    for table_name, part_id_set in table_name_to_objs_list.items():
        objs_set = _load_ids_from_db(db_fields_name_to_protocol_name, table_name, part_id_set, gtype=gtype, include_all=include_all)
        for o in objs_set:
            objs_dict[o.MD] = o

    return objs_dict

def report_info(c_file_name, c_file_type_id, c_deal_type):
    if c_deal_type not in set(["create", "push", "effect", "handle", "check"]):
        logger.error("invalid report params:[%s], [%s], [%s]"  %(c_file_name, c_file_type_id, c_deal_type))
    sql = "insert into t_file_deal_step (c_file_name,c_file_type_id,c_deal_type,c_start_time,c_end_time,c_operator,c_create_time) values('%s', '%s', '%s', now(), now(), '%s', now())"\
            % (c_file_name, c_file_type_id, c_deal_type, sys.argv[0])
    afects = DBQueryPool.get_instance("Search").execute_sql(sql)
    return afects 

def _get_fields_for_test():
    cur_keys_set = set(CSosoProtocolV3.KYES)
    db_fields_name_list = []
    db_fields_name_to_protocol_name = {}
    sql = "select * from t_data_sync_field_def where c_table_name = 't_direct_static'"
    rows = DBQueryPool.get_instance("d_complex_data").get_dict(sql)
    all_fields_set = set()
    for r in rows:
        protocol_name = r['c_sync_field']
        all_fields_set.add( protocol_name )
        db_field_name = r['c_sync_name']
        if protocol_name in cur_keys_set: 
            db_fields_name_list.append(db_field_name)
            db_fields_name_to_protocol_name[db_field_name] = protocol_name 

    print "all_fields_set-KEYS=%s" % (all_fields_set-cur_keys_set) 
    print "KEYS-all_fields_set=%s" % (cur_keys_set - all_fields_set) 

def test_redis_writer():
    # r = CRedisWriter(addr={'ip': '10.123.9.22', 'port': 6379}, key_ttl=60)
    r = CRedisWriter(zkname='vimix_test.redis.com')

    # key = 'test'
    # value = 'Hello World!'
    # assert(r.set_value(key, value) == True)
    # assert(r.get_value(key) == value)
    # assert(r.delete_key(key) == True)

    # test object
    objs_list = read_from_db_batch(['9dlcjs4slnbxku3'], gtype=2, include_all=False) # 精品区
    keys = r.add_medias_to_redis(objs_list)
    for key in keys:
        print(">> %s = %s" % (key, r.get_value(key)))

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", [ "--id"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)
    for o, a in opts:
        if o in ("-i", "--id"):
            id = a
            print "mysql -h10.240.64.138 -P3640 -ud_search_info -pae9baecd9 --default-character-set=utf8 d_complex_db_static_99\n"\
                    "select * from %s where c_media_id = '%s'\\G " % (_get_table_name_from_id(id), id)
            sys.exit(0)

    # _get_fields_for_test()
    # mapTabidInfo = get_tabid_map()
    # mapTypeToConfig = get_quality_config()
    # print "\nmapTabidInfo=\n"
    # for k, v in mapTabidInfo.items():
    #     print "k=%s, v=%s" % (k, v)

    # print "\nmapTypeToConfig=\n"
    # for k, v in mapTypeToConfig.items():
    #     print "k=%s, v=%s" % (k, v)


if __name__ == '__main__':
    #test_redis_writer()
    main()
