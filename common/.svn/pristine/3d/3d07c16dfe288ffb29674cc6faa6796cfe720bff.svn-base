#!/usr/bin/env python
# -*- coding: utf8 -*-

import sys
import time
import datetime

# Trans CSosoProtocolV3() to JsonDict() with PB protocol fields name
FIELD_LIST = [
    ('MD', 'string', 'id_mdsum'),
    ('IA', 'string', 'media_id'), # 不同IF分别取ID/IB/IA

    ('IG', 'int', 'comment'),
    ('IE', 'int', 'format_type'),

    ('WA', 'float', 'sort_weight'),
    ('WB', 'float', 'time_weight'),
    ('WC', 'float', 'hot_weight'),
    ('WD', 'int', 'has_omg_playright'),

    ('FA', 'int_array', 'data_source_type'),
    ('FB', 'int_array', 'local_playright'),
    ('FC', 'int_array', 'omg_playright'),
    ('FD', 'int_array', 'outsite_playright'),
    ('FE', 'int', 'video_type'),
    ('FF', 'int_array', 'pay_type'),
    ('FG', 'int_array', 'drm'),
    ('FH', 'int_array', 'play_source_ids'),
    ('FI', 'int_array', 'tv_boxs'),

    ('FJ', 'int', 'ages_range_id'),
    ('FK', 'int_array', 'special_data_type'),
    ('FN', 'int_array', 'children_tag_search_ids'),

    ('IA', 'string', 'column_id'),
    ('IB', 'string', 'cover_id'),
    ('IC', 'string', 'sdp_series_id'),
    ('IH', 'int', 'pay_type_old'),
    ('IJ', 'string', 'old_column_id_list'),
    ('IK', 'string', 'sdp_album_id'),
    
    ('TA', 'string', 'title'), # 不同IF分别取TA/TF/TC

    ('TA', 'string', 'father_column_title'),
    ('TC', 'string', 'father_cover_title'),
    ('TB', 'string', 'extra_title'),
    ('TD', 'string', 'english_name'),
    ('TE', 'string', 'alias'),
    # ('TS', 'string', 'search_alias'),

    ('VA', 'int', 'series_num'),
    ('VB', 'int', 'round_num'),
    ('VC', 'string', 'remake_version'),
    ('VD', 'string', 'edit_version'),
    ('VE', 'string', 'publisher'),
    ('VF', 'string', 'language'),
    ('VG', 'int', 'total_episode'),

    ('PA', 'string', 'actors'),
    ('PB', 'string', 'second_actors'),
    ('PC', 'string', 'guests'),
    ('PD', 'string', 'director'),
    ('PE', 'string', 'screen_writer'),
    ('PI', 'int_array', 'name_id_lists'),
    ('PN', 'string', 'role_names'),
    ('PM', 'string', 'role_groups'),
    ('PJ', 'int_array', 'role_name_ids'),
    ('PK', 'int_array', 'role_group_ids'),

    ('YA', 'string', 'type_name'),
    ('YB', 'string', 'subtype_name1'),
    ('YC', 'string', 'subtype_name2'),
    ('YD', 'string', 'subtype_name3'),
    ('YE', 'string', 'video_type_name'),
    ('YF', 'string', 'major_type'),
    ('YG', 'string', 'minor_type'),
    ('YI', 'int', 'content_type'),

    ('RA', 'string', 'tags'),
    ('RB', 'string', 'area'),
    ('RC', 'string', 'view_pot'),
    ('RD', 'string', 'view_pot2'),
    ('RE', 'string', 'familiar_roles'),
    ('RF', 'string', 'role_skills'),
    ('RG', 'string', 'awards'),
    ('RH', 'string', 'view_location'),
    ('RI', 'string', 'hot_program'),

    ('UB', 'int', 'hd'),

    ('DA', 'int', 'year'),
    ('DB', 'string', 'checkup_time'),
    ('DC', 'string', 'default_pic'),

    ('DD', 'string', 'url'),
    ('DE', 'int', 'age_range_start'),
    ('DF', 'int', 'age_range_end'),
    ('DU', 'int', 'time_long'),

    ('SD', 'int', 'user_score'),
    ('SE', 'int', 'cover_num'),
    ('SF', 'int', 'subscribe_num'),
    ('SG', 'int', 'search_rank'),

    ('CA', 'string', 'extra_buffera'),
    ('CB', 'string', 'extra_bufferb'),
    ('CC', 'string', 'extra_bufferc'),

    ('DG', 'timestamp', 'first_publish_time'),
    ('DH', 'timestamp', 'latest_publish_time'),
    ('DI', 'timestamp', 'latest_trailer_publish_time'),
    ('DJ', 'timestamp', 'latest_non_positive_publish_time'),
    ('DK', 'timestamp', 'premiere_publish_time'),
    ('DL', 'timestamp', 'china_publish_time'),
]

class JsonDict(dict):
    """general json object that allows attributes to be bound to and also behaves like a dict"""

    def __getattr__(self, attr):
        try:
            return self[attr]
        except KeyError:
            return None
            raise AttributeError(r"'JsonDict' object has no attribute '%s'" % attr)

    def __setattr__(self, attr, value):
        self[attr] = value

class Protocol2Json(object):

    def __init__(self, soso_obj):
        self.data = soso_obj

    def json(self, fields):
        result = JsonDict()
        IF = self._to_int(self.data.IF)

        for src, d_type, d_name in FIELD_LIST:
            # 只处理指定字段
            if not src in fields:
                continue

            if src in ['TC']:
                print src, d_name

            field = src

            # I类字段
            if d_name == 'media_id':
                if IF in [1,4,8,9,17,23, 5]:
                    field = 'ID'
                elif IF in [2,6,16]:
                    field = 'IB'
                elif IF in [3]:
                    field = 'IA'

            # T类字段
            elif d_name == 'title':
                if IF in [4,8, 6]:
                    field = 'TA'
                elif IF in [1,9,17,23]:
                    field = 'TF'
                elif IF in [2,16, 5]:
                    field = 'TC'
            elif d_name == 'extra_title':
                if IF == 5:
                    field = 'TA'

            # VB
            elif d_name == 'round_num':
                if IF in [1,4,8,9,17,23, 5]:
                    d_name = 'episode'
                elif IF in [2,6,16]:
                    d_name = 'round_num'

            # DB -> DB_ts
            elif d_name == 'checkup_time':
                result['checkup_time_ts'] = self._to_timestamp(self.data.DB)

            try:
                val = eval("self._to_%s(self.data.%s)" % (d_type, field))
                if not val in [None]:
                    result[d_name] = val

                ## val特殊处理
                # 栏目ID去掉类型
                if IF == 3 and d_name == 'media_id':
                    if result[d_name].find('_') != -1:
                        result[d_name] = result[d_name].split('_')[-1]
                # V+ url补充uin
                elif IF == 4 and d_name == 'url':
                    url = result[d_name]
                    if url.find('#') == -1:
                        result[d_name] += "#uin=%s" % result['media_id']

                # 据说是修复一个BUG: if(oMsg.iTypeid(FE) == 7 && oMsg.uiLength(DU) > 36000) oMsg.uiLength = 0;
                # zhijunluo: FE=7: 8,11,12,18,25,7
                FE = self._to_int(self.data.FE)
                if FE in [7,8,11,12,18,25] and d_name == 'time_long':
                    if result[d_name] > 36000:
                        result[d_name] = 0

            except Exception, e:
                sys.stderr.write("_to_%s(%s -> %s) failed: %s\n" % (d_type, d_name, field, e))

        return result

    def _to_int(self, val):
        if val in ['', None]:
            return None
        return int(val)

    def _to_int_array(self, val):
        if val in ['', None]:
            return None
        if type(val) in [unicode, str]:
            return [ self._to_int(v) for v in val.split(';') ]
        else:
            return [ self._to_int(val) ]

    def _to_float(self, val):
        if val in ['', None]:
            return None
        return float(val)

    def _to_string(self, val):
        if val in ['', None]:
            return None
        return str(val)

    def _to_timestamp(self, val):
        if val in ['', None]:
            return None
        if type(val) in [str, unicode]:
            val = datetime.datetime.strptime(val, '%Y-%m-%d %H:%M:%S')
        if type(val) == datetime.datetime:
            return int(time.mktime(val.timetuple()))
        else:
            return int(val)

def test():
    obj = JsonDict()
    obj.MD = 123456L
    obj.IF = 3
    obj.IA = '2_1234'
    obj.IB = 'xxx'
    obj.ID = 'ccc'
    obj.FA = '1;2;3'
    obj.TA = 'TA'
    obj.TC = 'TC'
    obj.TF = 'TF'
    obj.DB = '2016-11-08 15:59:45'

    p2j = Protocol2Json(obj)
    import json
    print json.dumps(p2j.json())

if __name__ == '__main__':
    test()
