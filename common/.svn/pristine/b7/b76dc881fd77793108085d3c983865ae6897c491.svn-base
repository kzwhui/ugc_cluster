#!/usr/bin/python
#encoding=utf8
import sys
import json
import time,datetime
import os 
import pickle
import sys

import logging
sys.path.append('../lib')
sys.path.append('../conf')
from db import DBQueryPool
import common
import l5sys
import cmem
from config import g_conf
from log import logger

def get_video_skip_rules(type, cate, vid, video_title, cover_title, map_non_first_vids_to_cover):
    """
    电影纪录片资讯MV分类等：同步长视频和碎片视频（有“_”“[*]”的长视频滤掉）
    电视剧动漫分类：同步长视频和碎片视频（有“_001”这样的分片视频进行合并，同步第一个，将标题处理为隐藏_001分片信息）
    综艺分类：同步长视频，看点，碎片视频（有“_”“[*]”的长视频滤掉)
    """
    if 2 == type or 3 == type:
        if 1 == cate:
            logger.debug("shield vid: type:[%d] cate:[%d] vid:[%s] video_title:[%s] cover_title:[%s]" % \
                         (type, cate, vid, video_title, cover_title))
            return True, video_title 
        #不是第一个视频，就屏蔽
        if cover_title and map_non_first_vids_to_cover.get(vid, None) is not None:
            logger.debug("shield vid: type:[%d] cate:[%d] vid:[%s] video_title:[%s] cover_title :[%s]" % \
                         (type, cate, vid, video_title, cover_title))
            return True, video_title
        ##是专辑第一个视频
        split_video_title_array = video_title.split("_")
        if len(split_video_title_array) > 1 and split_video_title_array[0] != video_title:
            logger.debug("replace title:[%s] [%s] " % (video_title, split_video_title_array[0]))
            video_title = split_video_title_array[0]#导出数据用，不是屏蔽逻辑使用

    elif 10 == type and 0 == cate:
        shield_str_in_title = set(['_', '[A]', '[B]', '[C]', '[D]', '[E]','[F]', '[G]','[H]'])
        for str in shield_str_in_title:
            if video_title.find(str) >= 0:
                logger.debug("shield vid: type:[%d] cate:[%d] vid:[%s] shield_str:[%s] video_title:[%s] cover_title :[%s]" % \
                             (type, cate, vid, str, video_title, cover_title))
                return True, video_title
    else:
        if 1 == cate:
            logger.debug("shield vid: type:[%d] cate:[%d] vid:[%s] video_title:[%s] cover_title :[%s]" % \
                         (type, cate, vid, video_title, cover_title))
            return True, video_title
        if 0 == cate:
            shield_str_in_title = set(['_', '[A]', '[B]'])
            for str in shield_str_in_title:
                if video_title.find(str) >= 0:
                    logger.debug("shield vid: type:[%d] cate:[%d] vid:[%s] shield_str:[%s] video_title:[%s] cover_title :[%s]" % \
                                 (type, cate, vid, str, video_title, cover_title))
                    return True, video_title

    return False, video_title

def cover_shield_condition(o):
    if 106 != o.get_channel_id():
        if (4 != o.cover_checkup_grade or o.copyright_id <= 0 or 3 == o.upload_src) \
           and (4 != o.data_checkup_grade or o.get_channel_id() not in (1, 2, 3)):
            logger.debug("shield cid:cover_checkup_grade[%s] copyright_id[%s] upload_src[%s] data_checkup_grade[%s] channel_id[%s]"\
                         %(o.cover_checkup_grade, o.copyright_id, o.upload_src, o.data_checkup_grade, o.get_channel_id()))
            return True
        if (4 == o.cover_checkup_grade and 350 == o.copyright_id and 3 != o.upload_src) \
           or (4 == o.data_checkup_grade and o.get_channel_id() in (1, 2, 3)) \
           and (hasattr(o, 'column_id') and o.column_id > 0):
            logger.debug("shield cid:cover_checkup_grade[%s] copyright_id[%s] upload_src[%s] data_checkup_grade[%s] channel_id[%s] column_id[%s]"\
                         %(o.cover_checkup_grade, o.copyright_id, o.upload_src, o.data_checkup_grade, o.get_channel_id(), o.column_id))
            return True
    return False

def video_shield_comon_condition(o):
    if 4 == int(o.state) and 3 != int(o.upload_src) and int(o.cate) >= 0 \
       and int(o.cate) <= 2 and int(o.type) > 0 and o.get_id():
        #2013-01-31cherylwang(王博) 10:48:30不从属于任何专辑的付费视频，过滤，是这个规则吗？hadesmo(莫璇) 10:50:49对
        if not o.get_cover_list() and 0 != int(o.drm):
            logger.info("shield vid:[%s] state:[%s] upload_src:[%s] cate:[%s] type:[%s] drm:[%s] " % \
                        (o.get_id(), o.state, o.upload_src, o.cate, o.type, o.drm)) 
            return True
    else:
        logger.info("shield vid:[%s] state:[%s] upload_src:[%s] cate:[%s] type:[%s]" % \
                    (o.get_id(), o.state, o.upload_src, o.cate, o.type)) 
        return True
    return False


class IMediaShieldLogic(object):

    def __init__(self, config = None):
        self.operation_shield_covers_set = set()
        self.operation_shield_video_set = set()

        self.cmem_api = cmem.CmemAPI()
        if not config:
            config = g_conf.OPERATOR_MEDIA_CMEM
        self.bid = config['bid']
        l5_config = config.get('l5')
        if l5_config:
            ret, qos = l5sys.ApiGetRoute(
                    {
                        'modId': l5_config['mod_id'],
                        'cmdId': l5_config['cmd_id'],
                        }, 0.5)
            if ret:
                raise Exception("get l5 route fail, ret=%s, qos=%s" % (ret, qos))
            addrs = [(qos['hostIp'], qos['hostPort'])]
            logger.info("l5=%s" % (addrs))
            self.cmem_api.config_server_addr(addrs)
        else:
            self.cmem_api.config_server_addr(config['addrs'])
        self.cmem_api.set_passwd(self.bid, config['pswd'])

    def init(self, id_list):
        self._get_operator_data()

    def is_shield(self, o):
        if not o:
            return True
        if o.is_shield():
            return True
        if not o.get_id():
            logger.error("no cid? impossible?[%s]" % o.id)
            return True
        if o.get_id() in self.operation_shield_covers_set:
            logger.debug("operation shield:[%s] " % o.get_id())
            return True
        if o.get_id() in self.operation_shield_video_set:
            logger.debug("operation shield:[%s] " % o.get_id())
            return True
        return False

    def _get_operator_data(self):
        (ret_key, value, cas) = self.cmem_api.get(self.bid, "videoID_shield")
        if not value:
            logger.error("no operator media ids ")
            return
        shields_array = json.loads(value)
        for r in shields_array:
            media_id = r.get('c_video_id', None)
            if not media_id:
                continue
            media_id = media_id.strip()
            if 11 == len(media_id):
                self.operation_shield_video_set.add(media_id)
            elif 15 == len(media_id):
                self.operation_shield_covers_set.add(media_id)
            else:
                raise Exception("ignore unknown media id:[%s] [%s]" % (r, media_id))
                #logger.error("ignore unknown media id:[%s]" % media_id)
        
        logger.info("len of self.operation_shield_video_set=%s, len of self.operation_shield_covers_set=%s" \
                    % (len(self.operation_shield_video_set), len(self.operation_shield_covers_set))) 

class CMediaCoverShieldLogic(IMediaShieldLogic):

    def __init__(self):
        IMediaShieldLogic.__init__(self)
        pass

    def is_shield(self, o):
        if IMediaShieldLogic.is_shield(self, o):
            return True
        return cover_shield_condition(o)
        #return False

class CMediaVideoShieldLogic(IMediaShieldLogic):

    def __init__(self):
        IMediaShieldLogic.__init__(self)
        self.map_first_video_to_time_longs = {}#第一个视频映射到所有vid的时长之和
        self.map_non_first_vids_to_cover = {}#非第一个vid映射到cid

    def init(self, id_list):
        IMediaShieldLogic.init(self, id_list)
        self.get_Vid2TimeAndNonFirstVideoMap(id_list)
        return 

    def is_shield(self, o):
        if IMediaShieldLogic.is_shield(self, o):
            return True
        #video_shield_comon_condition(o)下面的部分过滤条件修改时，要同步到这里的代码；目的是在union拉取时，只拉取少量的判断屏蔽的字段，提前过滤掉屏蔽的vid，减少更多字段的拉取
        if 4 == int(o.state) and 3 != int(o.upload_src) and int(o.cate) >= 0 \
           and int(o.cate) <= 2 and int(o.type) > 0 and o.get_id():
            #2013-01-31cherylwang(王博) 10:48:30不从属于任何专辑的付费视频，过滤，是这个规则吗？hadesmo(莫璇) 10:50:49对
            if not o.get_cover_list() and 0 != int(o.drm):
                logger.info("shield vid:[%s] state:[%s] upload_src:[%s] cate:[%s] type:[%s] drm:[%s] " % \
                            (o.get_id(), o.state, o.upload_src, o.cate, o.type, o.drm)) 
                return True
            #针对分段拼接，又从玉龙的拼接表中查询时长为0，说明视频状态不对，过滤掉
            if self.map_first_video_to_time_longs.get(o.get_id(), None) is not None and \
               self.map_first_video_to_time_longs[o.get_id()] > 0:
                logger.info("shield vid:[%s] state:[%s] upload_src:[%s] cate:[%s] type:[%s]" % \
                            (o.get_id(), o.state, o.upload_src, o.cate, o.type)) 
                return True
            cover_title = ''
            if o.get_cover_list() and o.father_cover:
                if o.father_cover.title:
                    cover_title = o.father_cover.title

            logger.debug("vid: type:[%s] cate:[%s] vid:[%s] video_title:[%s] cover_title:[%s]" % \
                         (o.get_channel_id(), o.cate, o.get_id(), o.title, cover_title))
            r, new_vid_title = get_video_skip_rules(int(o.get_channel_id()), int(o.cate), o.get_id(), o.title, cover_title, self.map_non_first_vids_to_cover)
            if not r:#这里会修改title
                o.title = new_vid_title
                return r
            return r
        else:
            logger.info("shield vid:[%s] state:[%s] upload_src:[%s] cate:[%s] type:[%s]" % \
                        (o.get_id(), o.state, o.upload_src, o.cate, o.type)) 
            return True
        return False

    def get_Vid2TimeAndNonFirstVideoMap(self, id_list):
        cur_timestamp = int(time.time())
        file_name = "../conf/%s.map_non_first_vids_to_cover" % sys.argv[0]
        from_time_stamp = self.load_dict_from_file(file_name)
        sql = "SELECT c_cid,c_vids,c_tls FROM t_cover_sec_new"
        if from_time_stamp > 0:
            sql = "SELECT c_cid,c_vids,c_tls FROM t_cover_sec_new where c_modifytime > from_unixtime(%s-3600)" % (from_time_stamp) 

        rows = DBQueryPool.get_instance("d_v_idx").get_dict(sql)
        cur_timestamp2 = int(time.time())
        cnt = 0
        new_items_cnt = 0
        for r in rows:
            vids = r['c_vids']
            cid = r['c_cid']
            time_longs = r['c_tls']
            if not vids: 
                continue
            vid_array_tmp = vids.split("+")
            vid_array = []
            for id in vid_array_tmp:
                if id:
                    vid_array.append(id)

            if len(vid_array) > 1:
                time_longs_plus = 0
                if time_longs:
                    time_longs_array = time_longs.split("+")
                    if time_longs_array and isinstance(time_longs_array, list):
                        for t in time_longs_array:
                            if t:
                                time_longs_plus += int(t)
                    elif isinstance(time_longs_array, int):
                        time_longs_plus += time_longs_array
                    else:
                        time_longs_plus += int(time_longs_array)

                self.map_first_video_to_time_longs[vid_array[0]] = time_longs_plus#这个array没有用，因为下一条日志没有打印过, 不存在time_longs_plus不合法的vid
                if time_longs_plus < 1:
                    logger.error("cid=%s, len=%s, tl=%s, {%s}" % \
                                 (cid, len(vid_array), time_longs_plus, time_longs))

                cnt = cnt - 1
                if cnt > 0:
                    logger.debug("cid=%s, vids=%s, tl=%s, {%s}" % \
                                 (cid, vids, time_longs_plus, time_longs))
                for idx in range(1, len(vid_array)):
                    self.map_non_first_vids_to_cover[vid_array[idx]] = cid
                    new_items_cnt = new_items_cnt + 1
               
        logger.info("new_items_cnt=%s, len(self.map_first_video_to_time_longs)=%s, len(self.map_non_first_vids_to_cover)=%s" % \
                    (new_items_cnt, len(self.map_first_video_to_time_longs.keys()), len(self.map_non_first_vids_to_cover.keys())))

        if new_items_cnt > 1000 or ((cur_timestamp2 - cur_timestamp) >= 1 and new_items_cnt > 20): 
            self.save_to_file(self.map_non_first_vids_to_cover, cur_timestamp, file_name)

    def load_dict_from_file(self, file_name):
        if not file_name:
            logger.error("no file name:[%s] " % file_name)
            return 0
        if not (os.path.exists(file_name)):
            return 0
        if not (os.path.getsize(file_name) > 0):
            return 0
        file = open(file_name, "r")
        is_first_line = True
        last_time_stamp = 0
        while True:
            line = file.readline()
            if not line:
                break
            line = line.strip()
            if is_first_line:
                last_time_stamp = int(line)
                logger.info("get last_time_stamp:[%s] " % last_time_stamp) 
                is_first_line = False
                continue
            line_array = line.split('\t')
            if len(line_array) != 2:
                logger.info("invalid line:[%s] " % line)
                continue
            self.map_non_first_vids_to_cover[line_array[0]] = line_array[1]

        file.close()
        if len(self.map_non_first_vids_to_cover.keys()) < 50000:
            logger.error("ignore invalid map_non_first_vids_to_cover:[%s] " % len(self.map_non_first_vids_to_cover.keys()))
            return 0
        return last_time_stamp 

    def save_to_file(self, to_be_save_map, first_line, file_name):
        if not to_be_save_map or not file_name:
            logger.error("no data or file?:[%s] [%s] " % (len(to_be_save_map.keys()), file_name))
            return
        outfile = open(file_name, "w")
        outfile.write("%s" % first_line)
        outfile.write("\n")
        for k, v in to_be_save_map.items():
            outfile.write("%s\t%s\n" % (k, v))

        outfile.close()
        logger.info("write file:[%s]" % first_line)
        return
def main():

    title = '其它_[A]llA天天向_上'
    r, new_title = get_video_skip_rules(2, 0, 'vid', title, 'cover_title', {})
    print 'r=%s, title=%s, new_title=%s' % (r,title, new_title)

if __name__ == '__main__':
    main()
