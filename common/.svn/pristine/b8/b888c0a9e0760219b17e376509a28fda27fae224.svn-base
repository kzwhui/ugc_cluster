#encoding:utf8
#引用该模块后需要先调用init_lib传入mongoDB所在的相对路径进行初始化，然后才能调用其他函数

import sys
sys.path.append('../conf')

from config import g_conf

mongo = None
def init_lib(relative_path):
    import sys
    sys.path.insert(0, relative_path)
    import pymongo
    global mongo
    mongo = pymongo

def connect(name):
    if not mongo:
        init_lib('../common/pymongo-2.7.1-py2.6-linux-x86_64.egg')
        #raise Exception(u'请先调用init_lib进行初始化')
    db_info = g_conf.MONGO_CONF.get(name, {})
    if not db_info:
        raise Exception(u'没有指定的MongoDB配置，name = %s' % name)
    conn = mongo.Connection('mongodb://%s:%s@%s:%s/%s' % (db_info['user'], db_info['password'], db_info['host'], db_info['port'],
        db_info['database']))
    return conn

class MongoConnPool:
    pool = {}   #{name: conn}
    @staticmethod
    def get_instance(name):
        if not MongoConnPool.pool.has_key(name):
            MongoConnPool.pool[name] = connect(name)
        return MongoConnPool.pool[name]
