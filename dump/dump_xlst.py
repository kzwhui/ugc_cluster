#encoding=utf8

import sys
sys.path.append('../common/')
from util import *
from db_wrapper import *


db_conf = {
        'host' : '127.0.0.1',
        'user' : 'root',
        'passwd' : 'zheng',
        'db' : 'd_ugc_video',
        'charset' : 'utf8'
        }
db_conn = DBWrapper(db_conf)

sql = "select c_title , c_title_s, c_description, c_uploader, c_upload_time , c_media_id, c_duration from t_ugc_video"
rows = db_conn.get_dict(sql)
print 'video info, len=', len(rows)

xls_data = []
title_flag = False
for row in rows:
    if not title_flag:
        title = []
        for k in row.keys():
            title.append(k.encode('utf8'))
        xls_data.append(title)
        title_flag = True

    data = []
    for v in row.values():
        data.append(("%s" % v).encode('utf8'))
    xls_data.append(data)

xlst_name = 'game_video_info'
data2xls(xls_data, xlst_name)
