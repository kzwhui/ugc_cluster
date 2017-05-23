#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import json
from bisect import bisect

MIN_QUALITY = 0.0
MAX_QUALITY = 1.0

# 分值定义
DU_BISECT = [ 1, 5+1, 60+1, 3*60+1, 10*60+1, 30*60+1, 60*60+1, 3*60*60+1 ]
DU_SCORES = [
    0,     # 小于1秒
    1,     # 小于5秒
   10,     # 1分钟内
   25,     # 1-3分钟
   30,     # 3-10分钟
   45,     # 10-30分钟
   65,     # 30-60分钟
   95,     # 1-3小时
   80,     # 3小时以上
   85,     # IF=专辑(cover)
  100,     # IF=栏目(column)
]
DU_WEIGHT = 0.20

FE_SCORES = {
    1:   80,  # 电影
    2:   80,  # 电视剧
    3:   80,  # 动漫
    4:   70,  # 体育
    5:   70,  # 娱乐
    6:   70,  # 游戏
    9:   80,  # 纪录片
   10:   80,  # 综艺
   23:   70,  # 新闻
   24:   60,  # 财经
}
FE_WEIGHT = 0.04

IG_WEIGHT = 0.15
FK_WEIGHT = 0.05
IE_WEIGHT = 0.08

YI_IG_SCORES = {
    1:  100,  # 正片
    3:   50,  # 预告片
    5:   30,  # 片花或特辑
    'column': 100, # IF=栏目
    'default': 50,
}
YI_SCORES = {
    1:   80,  # 正片
    3:   40,  # 预告片
    5:   20,  # 片花或特辑
    'column': 80, # IF=栏目
    'default': 40,
}
YI_WEIGHT = 0.10

CA_SCORES = {
    0:  100,    #tencent
    1:  90,     #iqiyi
    2:  60,     #tudou
    3:  40,     #pptv
    4:  30,     #sohu
    5:  50,     #letv
    7:  60,     #youku
    20: 50,     #56
    38: 80,     #bilibili
    39: 80,     #acfun
    41: 40,     #ku6
    'default':  0,  #default
}
CA_WEIGHT = 0.05

SOURCE_SCORES = {
    'feature':  100,    # 正片
}
SOURCE_WEIGHT = 0.02

def _to_int(val):
    try:
        return int(val)
    except Exception, e:
        return 0

def _format_FH_array(val):
    if val == None:
        return []
    elif type(val) == list:
        return [ int(v) for v in val ]
    elif type(val) == str:
        return [ int(v) for v in val.split(';') ]
    else:
        return [ int(val) ]

def _get_site_id(val, default=1000):
    try:
        CA_json = json.loads(val)
        return int(CA_json['play_site_id'])
    except Exception, e:
        return default

# 计算区间的线性值，如果scores是从小到大的，设置reverse=False
def _linear_value(value, bisect_list, scores_list, reverse=True, weight=1.0):
    index = bisect(bisect_list, value)
    # 头尾的直接取对应数值
    if (index <= 0) or (index >= len(bisect_list)):
        return scores_list[index]
    # 计算线性区间
    bisect_interval = bisect_list[index] - bisect_list[index-1]
    if reverse:
        score_interval = scores_list[index-1] - scores_list[index]
    else:
        score_interval = scores_list[index] - scores_list[index-1]
    rate = float(bisect_list[index] - value) / bisect_interval
    return scores_list[index] + rate * weight * score_interval

def _score_with_weight(score, score_max=100, weight=1.0):
    return float(score)/score_max * weight

def get_quality(obj):
    quality = 0.20
    debug_info = []

    debug_info.append("calc quality(%s, IF=%s)" % (obj.ID, obj.IF))

    # 时长DU
    DU = _to_int(obj.DU)
    if (obj.IF == 2):        # 专辑
        score_DU = _score_with_weight(DU_SCORES[-2], weight=DU_WEIGHT)
        quality += score_DU
        debug_info.append("Cover(DU) score=%.4f" % (score_DU))
    elif (obj.IF == 3):      # 栏目
        score_DU = _score_with_weight(DU_SCORES[-1], weight=DU_WEIGHT)
        quality += score_DU
        debug_info.append("Column(DU) score=%.4f" % (score_DU))
    else:
        if (DU <= 3600): # 时长小于60分钟，采用线性分值
            score_base = _linear_value(DU, DU_BISECT, DU_SCORES)
            score_DU = _score_with_weight(score_base, weight=DU_WEIGHT)
        else: # 时长超过1小时，采用量化后的分值
            index = bisect(DU_BISECT, DU)
            score_base = DU_SCORES[index]
            score_DU = _score_with_weight(score_base, weight=DU_WEIGHT)
        quality += score_DU
        debug_info.append("DU=%d score=%.4f(%.4f)" % (DU, score_DU, score_base))

    # 分类FE
    FE = _to_int(obj.FE)
    score_FE = _score_with_weight(FE_SCORES.get(FE, 60), weight=FE_WEIGHT)
    quality += score_FE
    debug_info.append("FE=%d score=%.4f" % (FE, score_FE))

    # 精品标志IG
    IG = _to_int(obj.IG)
    score_IG = _score_with_weight((100 if IG == 1 else 0), weight=IG_WEIGHT)
    quality += score_IG
    debug_info.append("IG=%d score=%.4f" % (IG, score_IG))

    # 原创标志FK
    FK = _to_int(obj.FK)
    score_FK = _score_with_weight((100 if FK == 3 else 0), weight=FK_WEIGHT)
    quality += score_FK
    debug_info.append("FK=%d score=%.4f" % (FK, score_FK))

    # 标准化/V+标志IE
    IE = _to_int(obj.IE)
    if IE in [3, 4]: # 已标准化
        score_IE = _score_with_weight(50, weight=IE_WEIGHT)
    elif IE == 6:    # V+上传
        score_IE = _score_with_weight(100, weight=IE_WEIGHT)
    else:
        score_IE = 0.0
    quality += score_IE
    debug_info.append("IE=%d score=%.4f" % (IE, score_IE))

    # 内容性质YI
    YI = _to_int(obj.YI)
    if IG == 1: # 精品内容分比非精品高
        if (obj.IF == 3):      # 栏目
            score_YI = _score_with_weight(YI_IG_SCORES['column'], weight=YI_WEIGHT)
            quality += score_YI
            debug_info.append("Column(YI) IG=1 score=%.4f" % (score_YI))
        else:
            score_YI = _score_with_weight(YI_IG_SCORES.get(YI, YI_IG_SCORES['default']), weight=YI_WEIGHT)
            quality += score_YI
            debug_info.append("YI=%d IG=1 score=%.4f" % (YI, score_YI))

    else:
        if (obj.IF == 3):      # 栏目
            score_YI = _score_with_weight(YI_SCORES['column'], weight=YI_WEIGHT)
            quality += score_YI
            debug_info.append("Column(YI) score=%.4f" % (score_YI))
        else:
            score_YI = _score_with_weight(YI_SCORES.get(YI, YI_SCORES['default']), weight=YI_WEIGHT)
            quality += score_YI
            debug_info.append("YI=%d score=%.4f" % (YI, score_YI))
            
    # 来源: FH=1且正片
    if 1 in _format_FH_array(obj.FH):
        if unicode(obj.YE) == u'正片':
            score_YE = _score_with_weight(SOURCE_SCORES['feature'], weight=SOURCE_WEIGHT)
            quality += score_YE
            debug_info.append("source FH=%s score=%.4f" % (obj.FH, score_YE))

    # 抓取站点CA
    if obj.IF == 23: # 外站UGC
        site_id = _get_site_id(obj.CA)
        score_CA = _score_with_weight(CA_SCORES.get(site_id, CA_SCORES['default']), weight=CA_WEIGHT)
        quality += score_CA
        debug_info.append("site_id=%d score=%.4f" % (site_id, score_CA))
    else:
        site_id = 0  # 站内
        score_CA = _score_with_weight(CA_SCORES.get(site_id, CA_SCORES['default']), weight=CA_WEIGHT)
        quality += score_CA
        debug_info.append("local site score=%.4f" % (score_CA))
        
    # 正片打压SG (因为weight影响，在求weight后再做SG打压)

    if quality < MIN_QUALITY: quality = MIN_QUALITY
    if quality > MAX_QUALITY: quality = MAX_QUALITY

    return quality, ", ".join(debug_info)

def test():
    test_set = [
        0, 1, 5, 30, 45, 60, 120, 180, 300,
        10*60, 20*60, 30*60, 45*60, 3600,
        1.5*3600, 2*3600, 2.5*3600, 3*3600, 4*3600,
    ]

    for time_long in test_set:
        sys.stdout.write("[%ss]" % (time_long))
        if (time_long < 3600):
            sys.stdout.write(" score=%.4f" % (_linear_value(time_long, DU_BISECT, DU_SCORES, weight=1.0)))
        else:
            sys.stdout.write(" score=%.4f" % (DU_SCORES[bisect(DU_BISECT, time_long)]))
        sys.stdout.write("\n")

if __name__ == '__main__':
    test()

