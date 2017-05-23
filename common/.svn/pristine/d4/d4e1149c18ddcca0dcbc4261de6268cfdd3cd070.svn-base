#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import datetime
import math
from bisect import bisect

# freshness取值[0.367879, 1]，取 1/e 为最小值是避免其太小，对结果产生决定性影响
BASE_FRESHNESS = 0.367879
MIN_FRESHNESS = 0.0
MAX_FRESHNESS = 1.0

CREATE_TIME_BISECT = [
    -20*12*30*86400, 1, 86400+1, 3*86400+1, 7*86400+1, 15*86400+1, 30*86400+1,
    3*30*86400+1, 6*30*86400+1, 12*30*86400+1, 2*12*30*86400+1, 30*12*30*86400+1,
]
CREATE_TIME_SCORES = [
    100,    # 未来10年
     99,    # 负数-未来10年
     95,    # 24小时内
     90,    # 1-3天
     80,    # 3-7天
     70,    # 7-15天
     50,    # 15-30天
     30,    # 1-3月
     20,    # 3-6月
     10,    # 6-12月
      5,    # 1-2年
      0,    # 2年-30年(线性)
      0,    # 30年前(固定)
]

CREATE_TIME_IG_BISECT = [ -20*12*30*86400, 1, 3*30*86400+1, 12*30*86400+1, 5*12*30*86400+1, 30*12*30*86400+1 ]
CREATE_TIME_IG_SCORES = [
    100,    # 未来10年
     99,    # 负数-未来10年
     95,    # 0-3月
     50,    # 3-12月
     25,    # 1-5年
      0,    # 5年-30年(线性)
      0,    # 30年前(固定)
]

CREATE_TIME_WEIGHT = 0.282121

UPDATE_VID_BISECT = [ 1, 3600+1, 86400+1, 3*86400+1, 7*86400+1 ]
UPDATE_VID_SCORES = [
    100,    # 负数，未来时间
     99,    # 1小时内
     90,    # 0-1天
     70,    # 1-3天
     50,    # 3-7天
      0,    # 7天前
]

UPDATE_TIME_WEIGHT = 0.35

def _to_datetime(val):
    if type(val) == datetime.datetime:
        return val
    elif type(val) in [str, unicode]:
        if len(val.strip().split(" ")) == 2:
            return datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        elif val.isdigit():         # timestamp
            return datetime.datetime.fromtimestamp(int(val))
        else:
            try:
                return datetime.datetime.strptime(val, "%Y-%m-%d")
            except:
                # 3yukuiad04ehr0w.publish_date = 2001年05月31日
                return datetime.datetime(1970, 1, 1, 0, 0, 0)
    elif (type(val) == int) and (val > 500000000):
        return datetime.datetime.fromtimestamp(val)
    else:
        # raise Exception("Unknow type(%s) = %s" % (type(val), val))
        return datetime.datetime(1970, 1, 1, 0, 0, 0)

def _year_to_datetime(val):
    try:
        if year < 1970 or year > 3000:
            year = 1970
        else:
            year = int(val)
    except:
        year = 1970
    return datetime.datetime(year, 1, 1, 0, 0, 0)

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

def _simple_linear_value(value, max_value):
    diff = float(max_value - value)
    # if diff <= 0.0: return 1.0
    return math.log10(diff + 1.000001) / math.log10(max_value + 1.000001)

def _score_with_weight(score, score_max=100, weight=1.0):
    return float(score)/score_max * weight

def get_create_time(obj, now):
    # 创建时间
    # 精品数据，取DK/DL里比当前时间早且最近的，没有取DG
    CREATE = None

    if obj.IG == 1:
        if obj.DK:
            DK_dt = _to_datetime(obj.DK)
            if DK_dt < now: # DK比当前时间小
                CREATE = DK_dt
                target_field = "DK"
                target_value = obj.DK
        if obj.DL:
            DL_dt = _to_datetime(obj.DL)
            if (DL_dt < now) and ((not CREATE) or (DL_dt > CREATE)): # DL比当前时间小且新于DK (国内重新上映)
                CREATE = DL_dt
                target_field = "DL"
                target_value = obj.DL

    # 非精品或无DK/DL的数据，取DG
    if not CREATE and obj.DG:
        CREATE = _to_datetime(obj.DG)
        target_field = "DG"
        target_value = obj.DG

    # 外站视频没有DG时，取DB
    if not CREATE and obj.DB:
        CREATE = _to_datetime(obj.DB)
        target_field = "DB"
        target_value = obj.DB

    # 如果都没有，尝试取DA (栏目:耐撕男女)
    if not CREATE:
        CREATE = _year_to_datetime(obj.DA)
        target_field = "DA"
        target_value = obj.DA

    return CREATE, target_field, target_value

def get_update_time(obj, now, with_extra=False):
    # 更新时间
    UPDATE = None
    target_field = None
    target_value = None

    # 先取DH，没有DH时取DG
    if obj.DH:
        DH_dt = _to_datetime(obj.DH)
        # 未来21天内上映，或者已经上映的取DH
        if DH_dt < now + datetime.timedelta(days=21):
            UPDATE = DH_dt
            target_field = "DH"
            target_value = obj.DH

    # 如果需要判断预告片更新DI/DJ
    if not UPDATE and with_extra and obj.DI:
        DI_dt = _to_datetime(obj.DI)
        if DI_dt < now + datetime.timedelta(days=7):
            UPDATE = DI_dt
            target_field = "DI"
            target_value = obj.DI
    if not UPDATE and with_extra and obj.DJ:
        DJ_dt = _to_datetime(obj.DJ)
        if DJ_dt < now + datetime.timedelta(days=7):
            UPDATE = DJ_dt
            target_field = "DJ"
            target_value = obj.DJ

    # 都没有则用1970初始UPDATE
    if not UPDATE:
        UPDATE = _to_datetime(None)

    return UPDATE, target_field, target_value

def get_freshness(obj, with_extra=False):
    freshness = 0.0
    debug_info = []

    if sys.version_info < (2, 7):
        raise Exception("total_seconds(): Python < 2.7 not supported yet...")

    debug_info.append("calc freshness(%s, IF=%s)" % (obj.ID, obj.IF))

    freshness = BASE_FRESHNESS
    now = datetime.datetime.now()

    target_field = None
    target_value = None

    # 创建时间
    # 精品数据，取DK/DL里比当前时间早且最近的，没有取DG
    CREATE, target_field, target_value = get_create_time(obj, now)
    timeago_CREATE = int((now-CREATE).total_seconds())

    ext = ''
    if obj.IG == 1:
        ext = ' IG(%d)' % obj.IG
        score_base = _linear_value(timeago_CREATE, CREATE_TIME_IG_BISECT, CREATE_TIME_IG_SCORES, weight=0.1)
    else:
        score_base = _linear_value(timeago_CREATE, CREATE_TIME_BISECT, CREATE_TIME_SCORES, weight=0.1)
    score_CREATE = _score_with_weight(score_base, weight=CREATE_TIME_WEIGHT)
    freshness += score_CREATE
    
    debug_info.append("%s=%s %ds ago score=%.4f(%.4f)%s" % (target_field, target_value, timeago_CREATE, score_CREATE, score_base, ext))


    # 更新时间
    if not obj.IF in [1,23]:
        UPDATE, target_field, target_value = get_update_time(obj, now, with_extra)
        dayago_UPDATE = int((now-UPDATE).total_seconds() / 86400)

        score_base = 0
        if obj.FE == 1:     # 电影类型 0-3月
            if dayago_UPDATE <= 90:
                score_base = 100 * _simple_linear_value(dayago_UPDATE, 90)
        elif obj.FE == 2:   # 电视剧 0-1月
            if dayago_UPDATE <= 30:
                score_base = 100 * _simple_linear_value(dayago_UPDATE, 30)
        elif obj.FE == 3:   # 动漫 0-45天
            if dayago_UPDATE <= 45:
                score_base = 100 * _simple_linear_value(dayago_UPDATE, 45)
        elif obj.FE == 10:  # 综艺 0-10天
            if dayago_UPDATE <= 10:
                score_base = 100 * _simple_linear_value(dayago_UPDATE, 10)
        else:               # 其他 0-1月
            if dayago_UPDATE <= 30:
                score_base = 100 * _simple_linear_value(dayago_UPDATE, 30)

        if (obj.FH == [2] and score_base > 0): # 只有站外源
            score_base *= 0.33
            debug_info.append("found FH=%s score(33%%)->%s" % (obj.FH, score_base))

        if target_field in ['DI','DJ']:
            score_base *= 0.5
            debug_info.append("hit %s score/2 -> %s" % (target_field, score_base))

        score_UPDATE = _score_with_weight(score_base, weight=UPDATE_TIME_WEIGHT)
        freshness += score_UPDATE
        debug_info.append("%s=%s update %d day ago score=%.4f(%.4f)" % (target_field, target_value, dayago_UPDATE, score_UPDATE, score_base))

    else: # 单视频更新
        # FIX BUG: 线上出现vid有DH时间的情况，ID=e00159phjsr MD=12441146588393668815
        # 改成取CREATE时间，而非更新时间
        UPDATE, target_field, target_value = get_create_time(obj, now)
        # UPDATE, target_field, target_value = get_update_time(obj, now, with_extra)
        diff_UPDATE = int((now-UPDATE).total_seconds())

        score_base = _linear_value(diff_UPDATE, UPDATE_VID_BISECT, UPDATE_VID_SCORES)
        if target_field in ['DI','DJ']:
            score_base *= 0.5
            debug_info.append("hit %s score/2 -> %s" % (target_field, score_base))

        score_UPDATE = _score_with_weight(score_base, weight=UPDATE_TIME_WEIGHT)
        freshness += score_UPDATE

        if score_base > 0:
            debug_info.append("%s=%s update %ds ago score=%.4f(%.4f)" % (target_field, target_value, diff_UPDATE, score_UPDATE, score_base))
        else:
            debug_info.append("vid/ugc(IF=%d) no update score" % (obj.IF))

    if freshness < MIN_FRESHNESS: freshness = MIN_FRESHNESS
    if freshness > MAX_FRESHNESS: freshness = MAX_FRESHNESS

    return freshness, ", ".join(debug_info)

def test():
    def timeago_to_string(timeago):
        if (timeago < 3600):
            return "%ds" % timeago
        elif (timeago < 86400):
            return "%.1fh" % (float(timeago)/3600)
        elif (timeago < 12*30*86400):
            return "%.1fd" % (float(timeago)/86400)
        else:
            return "%.1fy" % (float(timeago)/(12*30*86400))

    test_set = [
        5*-31136287, -31136287, -29753654, -1, 0, 
        1, 5, 30, 45, 60, 300, 3600, 3*3600, 6*3600, 18*3600, 86400,
        2*86400, 3*86400, 5*86400, 7*86400, 15*86400, 30*86400,
        2*30*86400, 6*30*86400, 12*30*86400, 18*30*86400, 24*30*86400,
        5*12*30*86400, 7*12*30*86400, 29*12*30*86400, 31*12*30*86400,
    ]

    for timeago in test_set:
        sys.stdout.write("[%s]" % timeago_to_string(timeago))
        sys.stdout.write(" CREATE(IG)=%.4f" % (_linear_value(timeago, CREATE_TIME_IG_BISECT, CREATE_TIME_IG_SCORES, weight=0.1)))
        sys.stdout.write(" CREATE=%.4f" % (_linear_value(timeago, CREATE_TIME_BISECT, CREATE_TIME_SCORES, weight=0.1)))
        sys.stdout.write("\n")

    for timeago in test_set:
        index = bisect(CREATE_TIME_BISECT, timeago)
        sys.stdout.write("%d bisect=%d\n" % (timeago, index))

    test_set_datetime = [
        1476415808, '1476415808', '2016-06-27', '2014-11-13 14:32:05',
    ]

    now = datetime.datetime.now()
    for dt in test_set_datetime:
        val = _to_datetime(dt)
        timeago = int((now-val).total_seconds())
        sys.stdout.write("timeago=%d, dt=%s: %s\n" % (timeago, dt, val))

    test_set_update = [
        -1, 1, 1800, 7200, 50000, 2*86400, 4*86400, 6*86400, 8*86400
    ]
    for update in test_set_update:
        sys.stdout.write("[%d] UPDATE=%.4f\n" % (update, _linear_value(update, UPDATE_VID_BISECT, UPDATE_VID_SCORES, weight=1.0)))

if __name__ == '__main__':
    test()
