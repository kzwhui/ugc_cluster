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

sql = "select c_title, c_uploader from t_ugc_video"
rows = db_conn.get_dict(sql)
print 'video info, len=', len(rows)

user_to_videos = {}
