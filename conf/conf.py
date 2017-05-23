#encoding=utf8

class GlobalConfig(object):
    '''
    全局配置文件
    '''
    DB_CONF = {
                'd_crawler_info' : {
                    'host' : '127.0.0.1',
                    'port' : 3306,
                    'user' : 'root',
                    'passwd' : 'zheng',
                    'db' : 'd_crawler_info',
                    'charset' : 'utf8'
                },

                'd_finance' : {
                    'host' : '127.0.0.1',
                    'port' : 3306,
                    'user' : 'root',
                    'passwd' : 'zheng',
                    'db' : 'd_finance',
                    'charset' : 'utf8'
                }
            }

    MAIL_CONF = {
        'host' : 'smtp.163.com',
        'port' : 25,
        'user' : 'xxxx',
        'passwd' : 'xxxx',
        'email' : 'xxxx@163.com'
    }

    REDIS_CONF = {
        'addr' : {
            'ip' : '127.0.0.1',
            'port' : 6379
        },
        'key_ttl' : 3600
    }

g_conf = GlobalConfig()
