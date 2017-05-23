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
sys.path.append('../common')
sys.path.append('../conf')
from log import logger
import union
import media_operator_lightweight
import my_md
from db import DBQueryPool

debug_flag = 0
class MediaAlum(union.MediaAlbumInterface):
    """
    当前的类的属性名对应到协议字段, id=>ID,title=>TF, 
    可以key=>str, key=>array, 也可以多个key对应一个value，但是这个value必须在MUTI_KEYS_FOR_ONE_VALUE, 然后会用分号分隔合并 
    """
    MAP_FIEDLS_TO_PROTOCOLV3 = {}
    USING_FATHER_KEYS = set()#使用父类的字段来替换，如IA,TA
    DEFAULT_VALUES = {} #默认值IF=>2
    MUTI_KEYS_FOR_ONE_VALUE = set()#协议字段可以由多个字段合并成的一个，如PB可以由guests,costar等几个字段合并
    PAY_TYPE_MAP = {}
    
    def __init__(self):
        self.father_column = None
        self.father_cover = None
        #self.id = None
        self.PAY_TYPE_MAP[1] = {4:4,5:3,6:2}
        self.PAY_TYPE_MAP[2] = {4:7,5:6,6:5}
        union.MediaAlbumInterface.__init__(self)
        self.play_ctrl_switch = None
        #self._libsimhash = cdll.LoadLibrary("../lib/libgen.so")

    def default_values(self):
        return 
        if not hasattr(self, 'id'):
            self.id = ''

        for channel_id, mapFieldsToProtocol in self.MAP_FIEDLS_TO_PROTOCOLV3.items():
            for k, v in mapFieldsToProtocol.items():
                if hasattr(self, k):
                    continue
                if isinstance(v, list):
                    v = v[0]

                if v in media_operator_lightweight.CSosoProtocolV3.DEFAULT_VALUES.keys():
                    self.__setattr__(k, media_operator_lightweight.CSosoProtocolV3.DEFAULT_VALUES[v])
                    continue
                if v in media_operator_lightweight.CSosoProtocolV3.INTERGE_TYPE_KEYS:
                    self.__setattr__(k, 0)
        #self.id = 'abc'

    def merge_father_attr_if_not_exist(self, o, father, attr_name, replace =  True):
        """
        replace: True表示在原有属性不存在时就替换；False表示只会Merge到原有属性上，不论原有属性是否存在
        """
        if not hasattr(o, attr_name) or not o.__getattribute__(attr_name):
            if hasattr(father, attr_name) and father.__getattribute__(attr_name):
                o.__setattr__(attr_name, father.__getattribute__(attr_name))

        elif not replace:
            o.__setattr__(attr_name, "%s;%s" % (o.__getattribute__(attr_name), father.__getattribute__(attr_name)))

    @staticmethod
    def get_father_class(obj):
        if obj.__class__ is MediaCover or obj.__class__ is MediaCoverForVideo:
            return MediaColumn
        if obj.__class__ is not MediaVideo:
            return None
        if hasattr(obj, 'cover_list') and obj.cover_list:
            return MediaCoverForVideo
        if hasattr(obj, 'column_id') and obj.column_id:
            return MediaColumn
        return MediaCoverForVideo
    @staticmethod
    def get_grandfather_class(obj):
        if obj.__class__ is MediaVideo:
            return MediaColumn
        return None

    def get_hash_code_for_string(self, string):
        my_str = "%s" % string
        if string and my_str :
            if not self._libsimhash:
                logger.error("no _libsimhash imported!!")
                return None
            if not isinstance(my_str, str):
                logger.error("string type invalid:[%s] [%s] " % (my_str, type(my_str)))
                return None
            #self._libsimhash.hello_world(c_int(len(my_str)))
            int_hash_code = self._libsimhash.Simhash_Project_CaculateHash(my_str, c_int(len(my_str)))
            print "int_hash_code = %s" % int_hash_code 


    def set_grandfather_obj(self, o):
        if MediaAlum.get_grandfather_class(self):
            self.father_column = o
        return

    def _has_muti_keys_of_protocol(self):
        m = MAP_FIEDLS_TO_PROTOCOLV3.get(0, None) 
        if not m:
            return set()
        s = set()
        for k, v in m.items():
            if k not in self.MUTI_KEYS_FOR_ONE_VALUE:
                s.add(v)

        return s

        #media_album
    def to_protocol_v3(self):
        if not self.MAP_FIEDLS_TO_PROTOCOLV3:
            logger.error("MediaAlum has no MAP_FIEDLS_TO_PROTOCOLV3:%s" % self)
            return None
        map_cur_channel_fields_to_protocol_v3 = self.MAP_FIEDLS_TO_PROTOCOLV3.get(0, None)
        if not map_cur_channel_fields_to_protocol_v3: 
            logger.error("MediaAlum has no MAP_FIEDLS_TO_PROTOCOLV3 mapping:%s" % o)
            return None
        #这里加入一些固定值的字段
        o = media_operator_lightweight.CSosoProtocolV3()
        if self.DEFAULT_VALUES:
            for k, v in self.DEFAULT_VALUES.items():
                o.__setattr__(k, v)

        if self.__class__ is MediaCover or self.__class__ is MediaVideo:
            #这里对一些特殊需要计算的字段进行处理，计算的方法都是统一的，不统一的在子类处理

            #YA/YB/YC/YD/YE字段的逻辑
            if self.category_map and 0 == (len(self.category_map) % 2) and len(self.category_map) > 2:
                attr_list = ['YA', 'YB', 'YC', 'YD']
                o.__setattr__('YE', self.category_map[1])
                j = 0
                for i in range(len(self.category_map) - 1, 1, -2):
                    if j >= len(attr_list):
                        break
                    o.__setattr__(attr_list[j], self.category_map[i])
                    j = j+1
            else:
                logger.error("invalid category_map:[%s] [%s]" % (self.get_id(), self.category_map))

            o.__setattr__('FH', self.get_play_source_FH())
            o.__setattr__('FI', self.get_play_source_FH())
            o.__setattr__('FF', self.get_pay_status_chansformed())
            o.__setattr__('MD', self.get_MD())
            o.__setattr__("FI", self.get_box_right_FI())
            o.__setattr__("FG", self.get_drm_FG())
            if self.has_self_playsource():
                o.__setattr__("WD", 1)
            else:
                o.__setattr__("WD", 2)

            o.__setattr__("YH", int(self.get_tabid_YH()))
            o.__setattr__("CA", self.get_cache1_CA())
            o.__setattr__("CB", self.get_cache2_CB())
            o.__setattr__("CC", self.get_cache3_CC())
            o.__setattr__("YI", MediaAlum.get_YI(self.category_map, self.type))
            if self.playright:
                o.__setattr__("FB", self.get_playright_FB())
                o.__setattr__("FC", o.FB)
                o.__setattr__("FD", o.FB)

            o.__setattr__("UB", self.get_sharp_UB())
        elif self.__class__ is MediaColumnDynamic or self.__class__ is MediaCoverDynamic:
            o.__setattr__('MD', self.get_MD())


        #set_of_muti_keys_to_one_protocol_key = self._has_muti_keys_of_protocol()
        #这里先对通用的映射进行转换
        for attr_name, mapping_fields_name in map_cur_channel_fields_to_protocol_v3.items():
            if (1 == debug_flag and (self.__class__ is MediaCover or self.__class__ is MediaVideo)) or hasattr(self, attr_name):
                if isinstance(mapping_fields_name, list): 
                    is_first = True
                    for field in mapping_fields_name:
                        self.merge_muti_values(attr_name, field, o, not is_first)
                        is_first = False
                else:
                    self.merge_muti_values(attr_name, mapping_fields_name, o)

        #对特定的频道使用不同字段的进行处理，如果一个字段在通用的有字段，然后在特定频道也有字段，这里就会用特定频道的字段进行覆盖，
        #这里要注意的两点：通用字段存在值，但是特定频道字段如果没值怎么处理？如果这个字段是多个字段合并起来的，这里要怎么覆盖？
        map_cur_channel_fields_to_protocol_v3 = self.MAP_FIEDLS_TO_PROTOCOLV3.get(self.get_channel_id(), None)
        if map_cur_channel_fields_to_protocol_v3: 
            for attr_name, mapping_fields_name in map_cur_channel_fields_to_protocol_v3.items():
                if (1 == debug_flag and (self.__class__ is MediaCover or self.__class__ is MediaVideo)) or hasattr(self, attr_name):
                    if isinstance(mapping_fields_name, list): 
                        is_first = True
                        for field in mapping_fields_name:
                            self.merge_muti_values(attr_name, field, o, not is_first)
                            is_first = False

                    else:
                        self.merge_muti_values(attr_name, mapping_fields_name, o)

        if not hasattr(o, 'SC') or not o.SC:#如果有周播放量就用周播放量，否则用昨日播放量
            if hasattr(self, 'view_yesterday_orig_count') and self.view_yesterday_orig_count:
                o.__setattr__("SC", self.view_yesterday_orig_count)


        #用父类的字段进行覆盖, 这里不一样的地方，如果有该频道的映射，就不会用频道0的映射，是2选一，上面的覆盖是先用频道0的，然后再用改频道的映射，是交集覆盖
        father_album = self.get_father_obj()
        if father_album and self.USING_FATHER_KEYS:
            father_o = father_album.to_protocol_v3()
            if father_o:
                father_keys = self.USING_FATHER_KEYS.get(self.get_channel_id(), None)
                if not father_keys:
                    father_keys = self.USING_FATHER_KEYS.get(0, None)

                if father_keys: 
                    for k in father_keys:
                        if hasattr(father_o, k):
                            o.__setattr__(k, father_o.__getattribute__(k))
                            if 'FF' == k and father_o.__getattribute__(k) is None: 
                                o.__setattr__(k, 1)

        return o

    def get_id(self):
        return self.id
    
    def preprocess(self):
        pass

    def get_special_rule_ids(self):
        """
        本函数主要逻辑，除了hbo之外都废弃, by zhijunluo, 2017-1-10
        c_date_flag各字节详细释义：
        第一个字节：&0x01==0非特殊专辑(所有用户可看)，&0x01==1特殊专辑(白名单用户可看)；
        第二个字节：==0人机都可修改，&0x01==1人工维护，>>1&0x01==1资料锁定；
        第三个字节：&0x01==1有UHD，>>1&0x01==1有杜比格式，>>2&0x01==1有软字幕，>>3&0x01==1有hevc编码。
        第四个字节：0x01：表示搜索不出现；0x02：表示列表不出现；0x04：表示推荐不出现
        """
        #hbo白名单之类的特殊规则的
        if not self.data_flag or 1 != (int(self.data_flag)&0x1): 
            return set()

        """
        从cyrilliang得知rule_id的含义:
            0--不限时间地区播放
            1--一类城市6时到24时限播
            2--hbo uin白名单限制
            3--一类城市二类城市6至24时限播
            4--仅限时间播放
            5--仅限地区播放
        """
        if not self.rule_id:
            return set()
        self.rule_id = "%s" % self.rule_id
        split_rules = self.rule_id.split("|")
        if len(split_rules) < 2:
            split_rules = self.rule_id.split(";")
        rules = set()
        for r in split_rules:
            if r > 0:
                rules.add(int(r))

        return rules

    def _get_special_rule_ids(self):
        #filter_id, 从1开始
        #未放开新代码
        new_rules = set()
        if not hasattr(self, 'filter_id'):
            return set()
        if not self.filter_id:
            return set() 
        logger.debug("id=[%s] play_ctrl_switch=[%s] filter_id=[%s]" % (self.get_id(), self.play_ctrl_switch, self.filter_id))
        self.filter_id = "%s" % self.filter_id
        split_rules = self.filter_id.split("+")
        for r in split_rules:
            new_rules.add(int(r))

        return new_rules



    def get_play_source_FH(self):
        fh = ''
        if self.has_self_playsource():
            if self.is_happy_copyright():
                fh = '5;'

            #http://tapd.oa.com/MediaContent/prong/stories/view/1010093801056665451 
            #上面的需求废弃了，有新的需求覆盖上面的逻辑：
            #需求：http://tapd.oa.com/10093801/prong/stories/view/1010093801058075403
            #离线数据提供方:cyrilliang
            #在线服务提供方:shawnsun
            #
            #zhijunluo(骆志军) 01-05 12:05:18
            #刚刚和crril面对面了解了下，有以下几个问题需要一起确认是否OK：
            #zhijunluo(骆志军) 01-05 11:53:54
            #1.union返回一个vid的规则是：1，2，3，服务返回2，搜索是可以搜索出这个vid
            #2. union返回一个vid的规则是：1，2，3，服务返回2，4，搜索是可以搜索出这个vid
            #3. union返回一个vid的规则是空的，服务返回任何值，搜索是可以搜索出这个vid
            #4. union返回一个vid的规则是1，2，3，服务返回空，搜索是可以搜索不出这个vid，它只能搜索出不带任何规则id的vid
            #zhijunluo(骆志军) 01-05 12:01:25
            #5. 老规则只保留HBO
            #6. HBO和新规则取并集
            #
            #shawnsun(孙志祥) 01-05 18:01:10
            #命令字 0xeed6 搜索或者推荐
            #zhijunluo(骆志军) 01-05 18:01:18
            #好的
            #zhijunluo(骆志军) 01-05 18:02:23
            #@cyrilliang(梁承希)规则ID有范围吗？
            #zhijunluo(骆志军) 01-05 18:03:13
            #为了兼容老规则，这里可能需要做下映射
            #cyrilliang(梁承希) 01-05 18:07:49
            #从1开始进行递增编号
            #cyrilliang(梁承希) 01-06 16:12:08
            #zhijun，刚才评估了下极端情况，还是可能需要抗量的，所以最终方案是用union来抗量，我这边单独分配一个数据源，你那边基本可以不用改动了
            #
            #挂到2001的上的字段filter_id
            #兼容新旧规则
            #play_ctrl_switch
            #cyrilliang(梁承希): 逻辑：为“否”或为空则不需要计算filter_id
            #zhijunluo(骆志军) 02-16 17:13:44
            #总结下刚刚电话你的结论：
            #1. 旧规则存在，直接使用旧规则数据，不需要计算新规则
            #2. 旧规则不存在，如果play_ctrl_switch为“是”，则需要计算新规则
            rules = self.get_special_rule_ids()
            if not rules and hasattr(self, 'play_ctrl_switch') and self.play_ctrl_switch and int(self.play_ctrl_switch) == 1543606:
                new_rules = self._get_special_rule_ids()
                if not new_rules:
                    new_rules = set([0])

                for r in new_rules:
                    if 0 == r:
                        fh = ('1;' if not fh else ('%s1;' % fh))
                    elif r > 0:
                        fh = ( ('%s;' % (r+10000))if not fh else ('%s%s;' % (fh, r+10000)))

            else:
                logger.info("id=[%s] play_ctrl_switch=[%s] " % (self.get_id(), self.play_ctrl_switch))
                if not rules:
                    rules = set([0])
                
                for r in rules:
                    if 0 == r:
                        fh = ('1;' if not fh else ('%s1;' % fh))
                    elif r < 10:
                        fh = ( ('%s;' % (r+10))if not fh else ('%s%s;' % (fh, r+10)))
                        #废弃这个逻辑，by zhijunluo， 2017-2-16
                        #if 6 == r:
                            #白名单数据需要兼容旧逻辑，多写一个值
                            #fh = ('6;' if not fh else ('%s6;' % fh))
                        #fh = (('%s;' % (r))if not fh else ('%s%s;' % (fh, r)))

        if self.has_third_playsource():
            if fh:
                fh = '%s;2;' % fh
            else:
                fh = "2;"

        return fh

    def get_playright_FB(self):
        category_value = int(self.get_category_value())
        playright_array = self.playright
        """
        albusliao(廖锡光) 2016-08-05 15:13:52
        刚和熊哥对了一下，应该是这样：
        1、未来的专辑会放在体育-奥运-未来电视 下，但是其对应的栏目会挂在 体育-奥运 下的几个类目里
        2、体育类型的专辑，搜索是以栏目为粒度搜出
        3、搜索会放出TV端没有版权的栏目，但是TV侧拿到栏目后，会取其中有版权的一期放出来。
        所以对于完全不能放出的栏目，只要该栏目下的专辑全是无版权，TV端就会过滤。
        对于可以放出栏目，不能放出部分专辑，TV端会直接进入有版权的那一期。
        -----------------
        所以摆了乌龙@zhijunluo(骆志军)@mingyangan(安明洋)这个需求应该是不用特殊处理。等@gelcoguo(郭鹏)鹏哥把不能显示的专辑挂到栏目下后，我们直接验证一下看看。
        #if category_value in set([11213, 11184]):
        #    playright_array = []
        #    for r in self.playright:
        #        if r not in set(['8', 8]):
        #            playright_array.append(r)
        """

        if isinstance(playright_array, list):
            return ";" . join(["%s" % r for r in playright_array])
        logger.debug("no playright for id:[%s] [%s] " % (self.get_id(), self.playright))
        return ''
    
    def get_box_right_FI(self):
        boxright = self.get_box_right()
        if not boxright:
            return ''
        if isinstance(boxright, list):
            return ";" . join(["%s" % (int(r) - 38) for r in boxright])
        else:
            logger.error("not a list instance:%s" % boxright)
        return ""

    def get_tabid_YH(self):
        if self.mapTabidInfo:
            return self.mapTabidInfo.get("%s" % (self.type), 7)
        return 0

    def get_drm_FG(self):
        if self.drm:
            return self.drm
        return 0

    #必须继承
    def get_category_value(self):
        return 0

    #必须继承
    def is_happy_copyright(self):
        #盗版数据的判断--True-盗版，False-非盗版
        return False

    #必须继承
    def get_pay_status_chansformed(self):
        mapPayStatus = self.PAY_TYPE_MAP.get(self.get_channel_id(), None)
        if not mapPayStatus:
            return 1
        return mapPayStatus.get(self.pay_status, 1)#TODO:坑，如果子类的付费字段不叫pay_status就会有问题，比如2003、2001叫法不同

    #必须继承, 只有具体的子类才知道它的father是谁，video的father是cover，cover的father是column
    def set_father_obj(self, o):
        pass

    #必须继承
    def get_father_id(self):
        return None
    #必须继承
    def get_grandfather_id(self):
        return None
    #必须继承
    def get_box_right(self):
        return None
    #必须继承
    def has_self_playsource(self):
        return 4 == self.cover_checkup_grade

    #必须继承
    def has_third_playsource(self):
        return False

    #必须继承
    def get_real_play_rules_id_list(self):
        """
        HBO白名单、盗版数据等
        """
        return None

    #必须继承
    def get_father_obj(self):
        return None
    #必须继承
    def get_channel_id(self):
        return self.type

    #必须继承
    def get_tv_box_FI(self):
        return '1'

    #必须继承
    def is_test_ids(self): 
        #过滤一些线上的测试数据，因为某些原因，线上存在一些不能被搜索到的测试数据
        return False

    #必须继承
    def is_shield(self): 
        #是否是被屏蔽的，屏蔽的就不会生成到文件和推送给引擎
        if self.is_test_ids():
            return True
        if self.is_happy_copyright():#盗版的综合区数据不给引擎
            logger.info("shield id by is_happy_copyright:[%s]" % self.get_id())
            return True
        return False

    #必须继承
    def get_sharp_UB(self):
        if not self.resolution:
            return 1
        if (int(self.resolution) + 1)/2 == 0:
            return 1
        return (int(self.resolution) + 1)/2 

    #必须继承
    def is_happy_copyright(self):
        #盗版数据的判断--True-盗版，False-非盗版
        return 350 == self.copyright_id 

    def get_MD(self):
        cmd = "../common/getmd5 -i %s" % self.get_id()
        res = os.popen(cmd).read()
        res = res.strip("\n")
        logger.info("res={%s}" % res)
        match_re_obj = re.match(r'^.*?md5\s+is\s+(\d+)$', res)
        if match_re_obj:
            md = match_re_obj.group(1)
            return md
        else:
            raise Exception("no MD caculated:[%s] ", self.get_id())
        return None

    def merge_muti_values(self, attr_name, protocol_field, o, is_same_key_with_last_key = False):
        if not hasattr(self, attr_name) or not self.__getattribute__(attr_name):
            return False
        new_value = self.__getattribute__(attr_name)
        if isinstance(new_value, list):
            new_value = ";" . join(["%s" % a for a in new_value])

        if not new_value:
            return False

        if is_same_key_with_last_key or \
           ( protocol_field in self.MUTI_KEYS_FOR_ONE_VALUE and hasattr(o, protocol_field) and o.__getattribute__(protocol_field)):
            old_value = o.__getattribute__(protocol_field)
            new_value = "%s;%s" % (new_value, old_value)

        o.__setattr__(protocol_field, new_value) 
        return True

    @staticmethod
    def get_YI(str, type):
        """
        含义参考了播放源出库的一个字段，不完全一样,代码inner_play_list.h的getNature函数
        """
        if not str:
            return 0
        if isinstance(str, int):
            return 0
        if isinstance(str, list):
            max_yi = 0
            for s in str:
                yi = MediaAlum.get_YI(s, type)
                if yi > 1:
                    return yi
                if max_yi < yi:
                    max_yi = yi

            return max_yi
        if str.find("预告片") >= 0:
            return 3
        if str.find("花絮") >= 0:
            return 4 
        logger.debug("get_YI:%s:%s" % (str, type))
        if type == 3 and (str.find("特辑") >= 0 or str.find("精华版") >= 0):
            return 1
        if str.find("正片") >= 0 or str.find("节目") >= 0 or str.find("网络电影") >=0:
            return 1
        if str.find("MV") >= 0:
            return 7
        if str.find("片花") >= 0 or str.find("特辑") >= 0:
            return 5
        return 0

    def get_cache1_CA(self):
        ca_dict={}
        ca_dict['type'] = int(self.type)
        if hasattr(self, 'real_exclusive'):
            ca_dict['re'] = int(self.real_exclusive)

        if hasattr(self, 'vertical_pic_url') and self.vertical_pic_url:
            pic2 = self.vertical_pic_url
            ca_dict['pic2'] = pic2.replace(".jpg", "_s.jpg")

        return json.dumps(ca_dict)

    def get_cache2_CB(self):
        cb_dict = {}
        cb_dict['dwr'] = ('+' . join(["%s" % r for r in self.downright]) if self.downright else '')
        if cb_dict['dwr']:
            cb_dict['dwr'] = "+%s+" % cb_dict['dwr'] 

        cb_dict['pr'] = ('+' . join(["%s" % r for r in self.playright]) if self.playright else '')
        if cb_dict['pr']:
            cb_dict['pr'] = "+%s+" % cb_dict['pr'] 

        cb_dict['drm'] = int(self.drm)
        cb_dict['updatemsg'] = self.get_cache3_CC()
        if self.__class__ is MediaVideo:#TODO:这个字段好像没啥用了，是否废弃之
            cb_dict['brief'] = self.desc
            cb_dict['endmsg'] = ''
        else:
            cb_dict['endmsg'] = ("%s" % self.time_long if self.time_long else '')

        formate_subtype = media_operator_lightweight.format_data_for_json(self.subtype) 
        if isinstance(formate_subtype, list):
            cb_dict['subtype'] = (';' . join(["%s" % r for r in formate_subtype]) if formate_subtype else '')
        else:
            cb_dict['subtype'] = formate_subtype

        cb_dict['playright'] = ''#TODO:strPlayTerminal, 重构发现精品已经没有这个字段了，那么综合区应该也不需要了
        return json.dumps(cb_dict)

    def get_cache3_CC(self):
        cc_dict = {}
        cc_dict['typeid'] = int(self.type)
        #if 3 == int(self.get_tabid_YH()) and self.__class__ is not MediaVideo:
        #    cc_dict['title'] = ("%s" % self.time_long if self.time_long else '')

        return json.dumps(cc_dict)


class MediaColumn(MediaAlum):
    UNION_TID_LIST = [607]
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'id': 'IA',
            'title' : 'TA',
            'presenter' : 'PA',
            'tutor' : 'PB',
            'presenter_id' : 'PI',
            'tutor_id' : 'PI',
            'program_id' : 'IC',
        },
    }

    MUTI_KEYS_FOR_ONE_VALUE = set(['PA','PB', 'PC', 'PD', 'PI'])

    def __init__(self):
        MediaAlum.__init__(self)

    def set_father_obj(self, o):
        logger.error("no to call me:%s:%s" % (o, o.get_id()))
        self.father_column = None

    def has_self_playsource(self):
        return True

    def has_self_playsource(self):
        return True

    #必须继承
    def is_happy_copyright(self):
        #盗版数据的判断--True-盗版，False-非盗版
        return False

        #media_column
    def to_protocol_v3(self):
        o = MediaAlum.to_protocol_v3(self)
        if not o:
            return o
        o.__setattr__("IA", "%s_%s" % (self.get_channel_id(), self.get_id()))
        return o

class MediaCover(MediaAlum):
    UNION_PRE_TID_SET = set([641])
    UNION_TID_LIST = [641,605,611,614]
    FATHER_CLASS = MediaColumn
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'id': 'IB',
            'second_title' : 'TB',
            'title' : 'TC',
            'type'  : 'YH',
            'year' : 'DA',
            'checkup_grade_time' : 'DB',
            'new_pic_hz' : 'DC',
            'url' : 'DD',
            'type' : 'FE',
            'langue' : 'VF',
            'type_name' : 'YA',
            'sub_genre' : 'YG',
            'main_genres' : 'YF',
            'pay_status' : ['IH'],
            'leading_actorX' : 'PA',
            'presenter' : 'PA',
            'race_teams' : 'PA',
            'stars' : 'PA',
            'singer_name' : 'PA',
            'costar' : 'PB',
            'race_stars' : 'PB', 
            'special_actor' : 'PC',
            'guests' : 'PC',
            'director' : 'PD',
            'guests_id' : 'PI',
            'leading_actor_id' : 'PI',
            'presenter_id' : 'PI',
            'stars_id' : 'PI',
            'costar_id' : 'PI',
            'director_id' : 'PI',
            'singer_id' : 'PI',
            'race_stars_id' : 'PI',
            #'' : 'PI',
            'area_name' : 'RB',
            'current_brief' : 'RC',
            #'resolution' : 'UB',
            'episode_all' : 'VG',
            'view_all_orig_count' : 'SB',
            'week_orig_allnum' : 'SC',
            #'average_score' : 'SE',
            'modify_time' : 'ZZMTIME',
        },
        1 : {
            'season' : 'VA',
        },
        2 : {
            'season' : 'VA',
        },

        10 : {
            'current_topic' : 'TB',
            'variety_episode' : 'VB',
        },
    }
    USING_FATHER_KEYS = {
        0 : set(['IA', 'TA']),
        #10 : set(['IA', 'TA', 'PB', 'PA']),
    }
    DEFAULT_VALUES = {
        'IE':4,
        'IF':2,
        'IG':0,
        'FA':'1',
        'SG':0,
        'DU':0,
        'X1':9,#媒资库的都是9，boke的是1，好像这个字段没用了
    }

    MUTI_KEYS_FOR_ONE_VALUE = set(['PA','PB', 'PC', 'PD', 'PI'])
    
    def __init__(self):
        MediaAlum.__init__(self)

    def get_class_type(self):
        return 2

    def set_father_obj(self, o):
        self.father_column = o

    def get_father_obj(self):
        return self.father_column

    def get_father_id(self):
        if self.column_id and int(self.column_id) > 0:
            return int(self.column_id)
        return None

    def get_box_right(self):
        return self.boxright

    def is_quality_data(self):
        if self.mapTypeToConfig:
            if self.mapTypeToConfig.get(self.type, None) is not None and (not hasattr(self, 'column_id') or not self.column_id):
                logger.debug("this id is quality_data:[%s] " % (self.get_id()))
                return True
            return False
        if self.__class__ is MediaCover:
            logger.debug("no mapTypeToConfig")

        return False

    #必须继承
    def is_shield(self): 
        #是否是被屏蔽的，屏蔽的就不会生成到文件和推送给引擎
        if MediaAlum.is_shield(self):
            logger.info("shield id by media_album:[%s]" % self.get_id())
            return True
        if self.is_quality_data():
            logger.info("shield id by is_quality_data:[%s]" % self.get_id())
            return True
        return False

    def get_category_value(self):
        return self.category_value

        #media_cover
    def to_protocol_v3(self):
        #预处理一些字段
        self.preprocess()
        o = MediaAlum.to_protocol_v3(self)
        if not o:
            return o
        self_id = self.get_id()
        if self.is_quality_data():
            o.IG = 1

        if 10 == self.get_channel_id() and self.get_father_obj():
            father_o = self.get_father_obj().to_protocol_v3()
            if father_o:
                self.merge_father_attr_if_not_exist(o, father_o, 'PA')
                self.merge_father_attr_if_not_exist(o, father_o, 'PB')
                self.merge_father_attr_if_not_exist(o, father_o, 'PI', False)

        return o

class MediaCoverForVideo(MediaCover):
    UNION_TID_LIST = [623]
    FATHER_CLASS = MediaColumn
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'id': 'IB',
            'c_second_title' : 'TB',
            'column' : 'TC',
            'original_title' : 'TD',
            'title_en' : 'TD',
            'alias' : 'TE',
            'type'  : 'YH',
            'type' : 'FE',
            'pay_status' : ['IH'],
            'drama_id' : 'IK',
            'series_id' : 'IC',
        },
        1 : {
            'title' : 'TC',
        },
        2 : {
            'title' : 'TC',
        },
        3 : {
            'title' : 'TC',
        },
        10 : {
            'current_topic' : 'TB',
        },
    }
    USING_FATHER_KEYS = {
        0 : set(['IA', 'TA', 'IC']),
        #10 : set(['IA', 'TA', 'PB', 'PA']),
    }
    DEFAULT_VALUES = {
        'IE':4,
        'IF':2,
        'IG':0,
        'FA':'1',
        'SG':0,
        'DU':0,
        'X1':9,#媒资库的都是9，boke的是1，好像这个字段没用了
    }

    MUTI_KEYS_FOR_ONE_VALUE = set(['PA','PB', 'PC', 'PD', 'PI'])

    def __init__(self):
        MediaCover.__init__(self)

    #必须继承
    def is_happy_copyright(self):
        #盗版数据的判断--True-盗版，False-非盗版
        return False

    def to_protocol_v3(self):
        o = MediaCover.to_protocol_v3(self)
        if not o:
            return o

        if self.title and (not hasattr(o, 'TC') or not o.TC):
            o.__setattr__('TC', self.title)

        o.__setattr__('FF', self.get_pay_status_chansformed())

        return o

class MediaColumnDynamic(MediaAlum):
    UNION_PRE_TID_SET = set()
    UNION_TID_LIST = [737]
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'column_id': 'IA',
            'c_allnumc' : 'SB',
            'c_ydnumc' : 'SC',
            'old_column_list' : 'IJ',
            'type' : 'FE',
        },
    }

    DEFAULT_VALUES = {
        'IF':3,
        }

    def __init__(self):
        MediaAlum.__init__(self)
        self.type = 0
        self.view_all_orig_count = 0
        self.week_orig_allnum = 0
        self.column_id = None

    def get_class_type(self):
        return 3

    def to_protocol_v3(self):
        if hasattr(self, 'columnview') and self.columnview:
            self.c_allnumc = self.columnview.get('c_allnumc', 0)
            self.c_ydnumc = self.columnview.get('c_ydnumc', 0)

        return MediaAlum.to_protocol_v3(self)

    def get_MD(self):
        return my_md.calculate_string_md("%s_%s" % (self.type, self.column_id))

class MediaCoverDynamic(MediaAlum):
    UNION_PRE_TID_SET = set()
    UNION_TID_LIST = [653]
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'id': 'IB',
            'view_all_orig_count' : 'SB',
            'week_orig_allnum' : 'SC',
            #'average_score' : 'SE',
        },
    }

    DEFAULT_VALUES = {
        'IF':2,
        }

    def __init__(self):
        MediaAlum.__init__(self)
        self.type = 0

    def get_class_type(self):
        return 2

class MediaVideoDynamic(MediaAlum):
    UNION_PRE_TID_SET = set()
    UNION_TID_LIST = [646]
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'id': 'ID',
            'view_all_orig_count' : 'SB',
            'view_week_orig_count' : 'SC',
            #'average_score' : 'SE',
        },
    }

    def __init__(self):
        MediaAlum.__init__(self)
        self.type = 0

    def get_class_type(self):
        return 1


class MediaVideo(MediaAlum):
    UNION_PRE_TID_SET = set([642])
    UNION_TID_LIST = [642,604,612,615,626]
    MAP_FIEDLS_TO_PROTOCOLV3 = {
        0 : {
            'id': 'ID',
            'second_title' : 'TB',
            'song_name' : 'TE',
            'title' : 'TF',
            'year' : 'DA',
            'create_time' : 'DB',
            'pic160x90' : 'DC',
            'url' : 'DD',
            'duration' : 'DU',
            'type' : ['FE'],
            'type_name' : 'YA',
            'sub_genre' : 'YG',
            'content_type' : 'YG',
            'main_genre' : 'YF',
            'series_num' : 'VA',
            'leading_actor' : 'PA',
            'presenter' : 'PA',
            'race_teams_name' : 'PA',
            'stars_name' : 'PA',
            'singer_name' : 'PA',
            'costar' : 'PB',
            'race_stars' : 'PB', 
            'written_by' : 'PB', 
            'hoster' : 'PB',
            'relative_stars' : 'PB',
            'special_actor' : 'PC',
            'guests' : 'PC',
            'composed_by' : 'PC',
            'director' : 'PD',
            'guests_id' : 'PI',
            'leading_actor_id' : 'PI',
            'presenter_id' : 'PI',
            'stars_id' : 'PI',
            'costar_id' : 'PI',
            'director_id' : 'PI',
            'singer_id' : 'PI',
            'stars' : 'PI',
            'race_stars_id' : 'PI',
            'relative_stars_id' : 'PI',
            'tag' : 'RA',
            'area_name' : 'RB',
            'plot_brief' : 'RC',
            'game' : 'RD',
            'relative_covers' : 'RD', 
            'famous_id' : 'RF',
            'tour_type' : 'RF',
            'mv_style' : 'RF',
            #'resolution' : 'UB',
            'episode' : 'VB',
            'cartoon_epnum' : 'VB',
            'belong_cid' : 'VD',
            "publish_company" : 'VE',
            "race_competition" : 'VE',
            'langue' : 'VF',
            'episode_all' : 'VG',
            'view_all_orig_count' : 'SB',
            'view_week_orig_count' : 'SC',
            #'average_score' : 'SE',
            'md5' : 'II',
            'modify_time' : 'ZZMTIME',
            'kb_atitle' : 'XF',
        },
        25 : {
            'gender' : 'RG',
            'brand' : 'RH',
            'famous_id' : 'RF',
            'style' : 'RE',
            'season' : 'RD',
        },
        26 : {
            'tour_site' : 'RD',
            'tour_topic' : 'RE',
            'tour_type' : 'RF',
            'tour_time' : 'RG',
        },
    }
    USING_FATHER_KEYS = {
        0 : set(['IA','IB', 'TA','TB','TC', 'TD', 'TE','IH', 'FF', 'IC', 'IK']),
        22 : set(['IA','IB', 'TA','TB','TC', 'TD', 'IH', 'FF', 'IC', 'IK']),
        200 : set(['IA','IB', 'TA','TB','TC', 'TD', 'IH', 'FF', 'IC', 'IK'])
        #10 : set(['IA', 'TA', 'PB', 'PA']),
    }
    DEFAULT_VALUES = {
        'IE':4,
        'IH':8,
        'IF':1,
        'IG':0,
        'FA':'1',
        'SG':0,
        'DU':0,
        'X1':9,#媒资库的都是9，boke的是1，好像这个字段没用了
    }

    MUTI_KEYS_FOR_ONE_VALUE = set(['PA','PB', 'PC', 'PD', 'PI', 'RD', 'RF', 'YG'])


    def __init__(self):
        self.is_vplus_uploader = False#上传者是否是v+用户
        self.is_first_author_video   = False#是否原创认证的视频
        self.liveInfoObj = None
        self.kb_aid = ""#快报数据的文章ID
        self.kb_mid = ""#快报数据的媒体ID
        self.kb_atitle = ""#快报数据的文章title
        self.kb_type = ""#快报视频数据的类型：4：白名单上传，5：普通用户上传
        MediaAlum.__init__(self)

    def get_class_type(self):
        return 1

    def pre_preprocess(self):
        if self.upload_src:
            self.upload_src = int(self.upload_src)
        
        #这个逻辑不知道什么年代的了，有badcase：e0013q0aeyn，http://tapd.oa.com/VideoSearch/prong/stories/view/1010141811059153465?url_cache_key=d9e80b568900bbc93e0607705c6be953&action_entry_type=story_tree_list
        #if self.title:
        #    split_video_title_array = self.title.split("_")
        #    if len(split_video_title_array) > 1 and split_video_title_array[0] != self.title:
        #        logger.debug("replace title:[%s] [%s] " % (self.title, split_video_title_array[0]))
        #        self.title= split_video_title_array[0]#导出数据用，不是屏蔽逻辑使用
        #支持快报的搜索。尝试从快报DB读取文章ID，媒体ID，文章标题
        table_name = media_operator_lightweight._get_kb_table_name_from_id(self.get_id())

        sql = "select c_aid,c_media_id,c_atitle from %s where c_entity_id='%s' and c_valid=1 order by c_modify_time desc limit 1" \
            % (table_name,self.get_id())

        rows = DBQueryPool.get_instance("d_kuaibao_data").get_dict(sql)
        if not rows:
            return

        for r in rows:
            if r['c_aid']:
                self.kb_aid = r['c_aid']
            if r['c_media_id']:
                self.kb_mid = r['c_media_id']
            if r['c_atitle']:
                self.kb_atitle = r['c_atitle']
        if self.kb_mid:
            #判断是否白名单媒体上传的文章，设置FA字段，4：白名单，5：非白名单
            sql = "select c_id from t_kuaibao_white_list where c_id=%s and c_valid=1 limit 1" % self.kb_mid

            rows = DBQueryPool.get_instance("d_white_list").get_dict(sql)
            if rows:
                self.kb_type = "4"
            else:
                self.kb_type = "5"
        logger.info("jack aid:%s,mid:%s,title:%s,type:%s" % (self.kb_aid,self.kb_mid,self.kb_atitle,self.kb_type))

        #media_video
    def to_protocol_v3(self):
        self.pre_preprocess()
        self.preprocess()
        self.special_logic()
        self.live_info_logic()

        o = MediaAlum.to_protocol_v3(self)
        if not o:
            return o

        if self.is_recommend_column():
            if o.FA:
                o.FA = "%s;%s" % (o.FA, '3')
            else:
                o.FA = '3'
        #写入快报视频的字段:类型FA，标题XF。aid和mid后续写入CA。vid不用新增
        if self.kb_aid:
            #万一没有获取到媒体ID，默认设置非白名单数据
            if not self.kb_type:
                self.kb_type = "5"
            if o.FA:
                o.FA = "%s;%s" % (o.FA, self.kb_type)
            else:
                o.FA = self.kb_type
#        if self.kb_atitle:
#            o.XF = self.kb_atitle;
        
        if 6 == self.upload_src:
            if self.upload_qq and self.is_vplus_uploader:
                o.IE = 6
            else:
                o.IE = 3

            #判断v+和原创的视频，v+的IE：6，CA加入uin，原创需要FK字段标记：1表示企鹅直播转点播视频，2表示儿童版视频，3表示原创视频
            #上面是原来的含义，据说这些值不会有冲突, 有冲突就会有问题
            if self.is_first_author_video: 
                o.FK = 3

        self.live_info_logic_merge(o)

        if self.second_title and (not hasattr(o, 'TB') or not o.TB):
            o.TB = self.second_title 

        if self.get_father_obj():
            father_o = self.get_father_obj().to_protocol_v3()
            if father_o:
                self.merge_father_attr_if_not_exist(o, father_o, 'TE')

        #IL第一位设置为1如果是完整版
        if hasattr(self, 'c_full') and self.c_full:
            o.IL = 0x1
            
        #使用FK字段标记VR视频：1表示企鹅直播转点播视频，2表示儿童版视频，3表示原创视频，4：VR视频
        if self.data_flag and 0x1 == ((int(self.data_flag)>>23)&0x1):
            if o.FK:
                o.FK = '%s;4' % (str(o.FK))
            else:
                o.FK = '4'
        '''
        ATTENTION!!!!!!!!!!<<<此项改动产品又说不用改，后续如果又要，直接放开即可>>>
        #1.union单视频增加了drm字段，0免费，1和2 付费。搜索同步协议现有FF字段最大值是17，预留一部分，drm1对应FF值31，2对应FF值32.
        #2.如果单视频drm是0，直接置FF为1，即免费
        #3.如果单视频drm非0，先判断视频所在专辑，如果付费，则单视频FF置为专辑FF值相同，否则单视频FF置31或32（由drm映射）.
        #现有逻辑，单视频付费就是跟专辑走的，所以只需要如下改就可以了。
        if int(self.drm) == 0:
            o.FF = 1
        elif o.FF <= 1:
            if int(self.drm) == 1:
                o.FF = 31
            elif int(self.drm) == 2:
                o.FF = 32
        '''
        
        return o

    def get_cover_list(self):
        if self.cover_list:
            if isinstance(self.cover_list, list):
                return self.cover_list
            return [self.cover_list]
        return []

    def special_logic(self):
        """
        判断v+和原创的视频，v+的IE：6，CA加入uin，原创需要FK字段标记：1表示企鹅直播转点播视频，2表示儿童版视频，3表示原创视频
        zhijunluo(骆志军) 03-03 11:50:20
        jim，扫描媒资db取出的单视频数据，哪个字段标记这个单视频是来自UGC的？
        jimshi(石晶翔) 03-04 10:42:10
        c_src=6
        1006取的判断原创字段的值的含义, rtx by keaneliu
        enum E_DELIVER_METHOD
        {
        V_DELIVER_DEFAULT   = 0,    // 默认
        V_DELIVER_ORIGINAL  = 1,    // 原创
        V_DELIVER_REPRINT   = 2,    // 转载
        V_DELIVER_OTHER     = 3,    // 其它
        }
        """
        if 6 != self.upload_src or not self.upload_qq:
            return False
        union_helper = union.UnionHttp()
        vvideoInfoObj = union_helper.load_objects(union.VVideoInfo, self.get_id()) 
        if vvideoInfoObj: 
            if 1 == vvideoInfoObj.deliverMethod:
                self.is_first_author_video = True
                logger.info("is_first_author is True:[%s] [%s]" % (self.get_id(), self.upload_qq))
            else:
                logger.info("is_first_author is False:[%s] [%s]" % (self.get_id(), self.upload_qq))

        vplusInfoObj = union_helper.load_objects(union.VplusInfo, self.upload_qq, True) #如果不是V+，就会获取不到数据，这种错误是正常的错误
        if vplusInfoObj:
            if hasattr(vplusInfoObj, 'nick') and vplusInfoObj.nick:
                self.is_vplus_uploader = True
                logger.info("is_vplus_uploader is True:[%s] [%s]" % (self.get_id(), self.upload_qq))
            else:
                logger.info("is_vplus_uploader is False:[%s] [%s]" % (self.get_id(), self.upload_qq))

        return True
        #media_video
    def live_info_logic(self):
        """
        企鹅直播需求, by zhijunluo
        * figecheng(程起飞) 02-23 16:49:27
        * TC：直播的title
        * 相关明星：PB，PI
        *
        * v+昵称：暂时不需要
        * 根据需求改动
        * zhijunluo(骆志军) 02-24 15:46:27
        * FE增加2700#FE不能有多个值, TODO:加入到buffer里面
        * zhijunluo(骆志军) 02-24 15:50:39
        * TA：直播运营标题
        * TC：直播标题
        """
        live_debug_flag = 0
        if 1 == live_debug_flag:
            if 1 != debug_flag:
                live_debug_flag = 0

        if not self.live_id and 1 != live_debug_flag:
            return False
        union_helper = union.UnionHttp()
        if 1 == live_debug_flag:
            liveInfo = union_helper.load_objects(union.SpheniscidaeLiveInfo, 2000709133)#TODO: for test
        else:
            liveInfo = union_helper.load_objects(union.SpheniscidaeLiveInfo, self.live_id)
        if not liveInfo:
            return False

        if 1 == live_debug_flag:
            union.print_debug_info([liveInfo])
        
        if not liveInfo.super_title and not liveInfo.title:
            return False
        self.liveInfoObj = liveInfo 
        return True

    def live_info_logic_merge(self, o):
        if not self.liveInfoObj:
            return False
        if not self.liveInfoObj.super_title and not self.liveInfoObj.title:
            return False

        if hasattr(o, 'FK') and o.FK > 1:
            logger.error("comflict FK:[%s] old FK:[%s] new FK:[%s]" % (self.get_id(), o.FK, 1))

        o.FK = 1
        if self.liveInfoObj.super_title:
            o.TC = self.liveInfoObj.super_title

        if self.liveInfoObj.title:
            o.TA = self.liveInfoObj.title

        if self.liveInfoObj.guest:
            data = json.loads(self.liveInfoObj.guest)
            if isinstance(data, list):
                for row in data:
                    id_type = row.get('id_type', None)
                    name_id = row.get('id', None)
                    #name_id = row.get('vplus_id', None)
                    name = row.get('name', None)
                    #name = row.get('vplus_name', None)
                    if 1 == debug_flag:
                        print "id_type = %s, name_id=%s, name=%s, row=%s" % \
                                (id_type, name_id, name, row)

                    if id_type is not None and id_type == 0:
                        if name:
                            if o.PB:
                                o.PB = "%s;%s" % (name, o.PB)
                            else:
                                o.PB = name
                        if name_id:
                            if o.PI:
                                o.PI = "%s;%s" % (name_id, o.PI)
                            else:
                                o.PI = "%s" % name_id

                    elif 1 == debug_flag:
                        print "id_type invalid:%s" % id_type

            elif 1 == debug_flag:
                print "live guest not an array"


    def is_recommend_column(self): 
        #蛋疼的需求，说要提供特定的视频集合供专题页搜索, by grit
        if self.column_id:
            if int(self.column_id) in set([11470, 11464, 11324]):
                return True
        return False

    def get_category_value(self):
        return self.c_category_value

    #必须继承
    def get_pay_status_chansformed(self):
        return 1
    """
        mapPayStatus = self.PAY_TYPE_MAP.get(self.get_channel_id(), None)
        if not mapPayStatus:
            return 1
        return mapPayStatus.get(self.pay_status, 1)#TODO:坑，如果子类的付费字段不叫pay_status就会有问题，比如2003、2001叫法不同
    """

    #必须继承, 只有具体的子类才知道它的father是谁，video的father是cover，cover的father是column
    def set_father_obj(self, o):
        self.father_cover = o

    def get_grandfather_id(self):
        if self.column_id:
            str = self.column_id
            try:
                return int(str)
            except Exception,e:
                logger.error(traceback.format_exc())
                logger.error("to int err:[%s] " % str)
                return 0
        return None
    #必须继承
    def get_father_id(self):
        if self.cover_list:
            if isinstance(self.cover_list, list):
                return self.cover_list[0]
            return self.cover_list
        if self.column_id and int(self.column_id) > 0:#坑吗？
            return int(self.column_id)
        return None
    #必须继承
    def get_box_right(self):
        return self.boxright
    #必须继承
    def has_self_playsource(self):
        return 4 == self.state

    #必须继承
    def has_third_playsource(self):
        return False

    #必须继承
    def get_real_play_rules_id_list(self):
        """
        HBO白名单、盗版数据等
        """
        return None

    #必须继承
    def get_father_obj(self):
        return self.father_cover

    #必须继承
    def get_tv_box_FI(self):
        return '1'

    def is_test_ids(self):
        if self.data_flag and 0x1 == ((int(self.data_flag)>>24)&0x1):
                return True
        return False

    def get_cache1_CA(self):
        ca_dict={}
        ca_dict['type'] = int(self.type)
        if not self.cover_list:
            ca_dict['cids'] = '' 
        elif isinstance(self.cover_list, list):
            ca_dict['cids'] = '+' . join(["%s" % cid for cid in self.cover_list])
            if ca_dict['cids']: 
                if len(self.cover_list) > 1:
                    ca_dict['cids'] = "+%s+" % ca_dict['cids']
                else:
                    ca_dict['cids'] = "+%s" % ca_dict['cids']

        else:
            ca_dict['cids'] = "+%s" % self.cover_list#TODO：注意所有CA、CB、CC的+“等奇葩格式都是为了使之和旧程序做对比的diff的


        if self.liveInfoObj:
            if self.liveInfoObj.super_title or self.liveInfoObj.title:
                ca_dict['special_type'] = "1" 

        ca_dict['uin'] = ('' if not self.is_vplus_uploader or not self.upload_qq else ("%s" % self.upload_qq))
        #写入快报数据的文章ID和媒体ID
        if self.kb_aid:
            ca_dict['aid'] = self.kb_aid
        if self.kb_mid:
            ca_dict['mid'] = self.kb_mid

        return json.dumps(ca_dict)

    def get_cache3_CC(self):
        cc_dict = {}
        cc_dict['typeid'] = int(self.type)
        if  22 == self.type and self.singer_name:
            new_val = media_operator_lightweight.format_data_for_json(self.singer_name)
            cc_dict['title'] = "%s" % new_val 

        return json.dumps(cc_dict)


def main():
    logger.info("hello world=%s" % sys.argv[0])
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hi:", ["--help","--ids"])
    #except getopt.GetoptError as err:
    except Exception,e:
        # print help information and exit:
        print str(e) # will print something like "option -a not recognized"
        sys.exit(2)

    ids = None
    for o, a in opts:
        if o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--ids"):
            ids = a
        else:
            assert False, "unhandled option"

    string = 'abcasdfasdfakldsjfa;lsjdflkjasl;djfl;askjdl;fjk'
    m = MediaAlum()
    #m.get_hash_code_for_string(string)
    #id = ids
    #c = MediaCover()
    #v = MediaVideo()
    #l = MediaColumn()
    #u = union.UnionHttp()
    #obj = u.load_objects(MediaCover, id) 

    #father = u.load_objects(MediaColumn, obj.column_id)
    #if father:
    #    obj.set_father_obj(father)
    #else:
    #    print "no father column"

    #union.print_debug_info([obj]) 
    #union.print_debug_info([father]) 

    #obj.to_protocol_v3().process()
    #objs = u.load_objects(MediaVideo, ["f0020p3sdbn"]) 
    #union.print_debug_info([objs[0].to_protocol_v3()]) 


if __name__ == '__main__':
    main()
