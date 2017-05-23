#!/usr/bin/python
#encoding=utf8
import sys
import json
import logging
import time
import datetime
import commands
import getopt
import traceback
sys.path.append('../common/redis.zip')
import redis
sys.path.append('../conf')
from config import g_conf
sys.path.append('../common')
from log import logger
sys.path.append('/usr/local/zk_agent/names/')
from nameapi import getHostByKey

def attemp_get_host_from_zk(host, port):
    real_host = host
    real_port = port
    ret, ip, port = getHostByKey(host)
    if ret == 0:
        real_host = ip
        real_port = port
    return (real_host, real_port)

class CRedisHelper(object):
    def __init__(self, conf = None):
        self.pool = self.get_redis_pool(conf)
        self.r = None

    def get_redis_pool(self, config = None):
        if not config: 
            config = g_conf.REDIS_CONF 
        zk_name = config.get('zk_name')
        if zk_name: 
            ip, p = attemp_get_host_from_zk(zk_name, 0)
            if ip and p:
                pool = redis.ConnectionPool(host=ip, port=p) 
                return pool 
            log.error("zk name fail:%s, use addr" % (zk_name))

        addr = config.get('addrs')
        ip = addr['ip']
        port = addr['port']
        logger.info("ip=%s, port=%s" % (ip, port))
        pool = redis.ConnectionPool(host=ip, port=port)  
        return pool

    def get_redis(self):
        if self.r:
            return self.r

        if not self.pool:
            self.pool = self.get_redis_pool()

        if not self.pool:
            logger.error("no redis pool")
            return None
        self.r = redis.Redis(connection_pool=self.pool)
        return self.r

    def set_value(self, key, value, ttl=None):
        r = self.get_redis()
        if not r:
            logger.error("no redis connected")
            return "no redis error"
        if not ttl:
            return r.set(key, value)
        else:
            return r.setex(name=key, time=ttl, value=value)

    def get_value(self, key):
        r = self.get_redis()
        if not r:
            logger.error("no redis connected")
            return "no redis connected"
        return r.get(key)

    def delete_key(self, key):
        r = self.get_redis()
        if not r:
            logger.error("no redis connected")
            return "no redis connected"
        return r.delete(key)

    ##  Usage:
    #   pipe = get_pipeline()
    #   for key, value in kvmap.items():
    #       pipe_set_value(pipe, key, value)
    #   pipe.execute() # important !!!
    def get_pipeline(self, transaction=False):
        r = self.get_redis()
        if not r:
            logger.error("no redis connected")
            return "no redis connected"
        return r.pipeline(transaction=transaction)

    def pipe_set_value(self, pipe, key, value, ttl=None):
        if not ttl:
            return pipe.set(key, value)
        else:
            return pipe.setex(name=key, time=ttl, value=value)

def test():
    key = "hellow"
    value = "zhijunluo"
    r = CRedisHelper()
    v = r.set_value(key, value) 
    v = r.get_value(key) 
    print "k=%s, v=%s" % (key, v)

def main():
    test()

if __name__ == "__main__":
    main()

