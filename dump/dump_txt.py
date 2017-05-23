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

f = open('video_info.txt', 'w')
title_flag = False
for row in rows:
    if not title_flag:
        for k in row.keys():
            f.write('%s\t' % k.encode('utf8'))
        f.write('\n')
        title_flag = True

    for v in row.values():
        data = '%s' % v
        f.write('%s\t' % data.encode('utf8'))
    f.write('\n')

f.close()

print 'over'
