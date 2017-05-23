#!/usr/bin/python
#encoding=utf8
import re
import pickle
import sys
import json
import time,datetime
import traceback
import logging
import collections
sys.path.append('../lib')
sys.path.append('../common')
sys.path.append('../conf')
sys.path.append('/usr/local/zk_agent/names/')
from nameapi import getHostByKey
import l5sys
import cmem
from config import g_conf
from log import logger

def attemp_get_host_from_zk(host, port):
    real_host = host
    real_port = port
    ret, ip, port = getHostByKey(host)
    if ret == 0:
        real_host = ip
        real_port = port
    return (real_host, real_port)

class PlaySourceReader(object):
    def __init__(self, config = None):
        self.cmem_api = cmem.CmemAPI()
        if not config:
            config = g_conf.PLAY_SOURCE_TMEM 
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

    def read(self, media_id):
        tmem_key = "pl_index_%s" % (media_id)
        index_root = self._get_json_from_cmem(tmem_key)
        if not index_root:
            return []

        #for site_play_source in index_root['data']:
            #episode_list, update_time = self._read_site_play_source(site_play_source)
            #site_play_source['episode_list'] = episode_list
            #site_play_source['update_time'] = update_time
            #site_play_source['site'] = site_play_source['source']
        logger.debug('tmem_key=%s media_id=%s, play_source=%s' % (tmem_key, media_id, json.dumps(index_root['data'], ensure_ascii=False, encoding='utf8')))
        return index_root['data']

    def _get_from_cmem(self, key):
        #print "key=%s" % key
        (ret_key, value, cas) = self.cmem_api.get(self.bid, key)
        return value

    def _get_json_from_cmem(self, key):
        try:
            value = self._get_from_cmem(key)
            return json.loads(value)
        except cmem.CmemError, e:
            if e[0] != -13200:
                print "no such key=%s" % key
                logger.error(traceback.format_exc())
                logger.error("reraise it")
                #raise e
        return None

    def _read_site_play_source(self, index):
        episode_list = []
        update_time = None
        for page in index.get('pages', []):
            page_json = self._get_json_from_cmem(page['key'])
            update_time = page_json['update_time']
            episode_list.extend(page_json['data'])
        for e in episode_list:
            e['episode_number'] = int(e['episode_number'])
        return episode_list, update_time

    def has_inner_positive(self, play_source):
        for site_play_source in play_source:
            channel_id = "%s" % site_play_source['type']
            if site_play_source['site'] == 'qq' and channel_id == "1":
                return True

    @staticmethod
    def get_latest_episode_number(site_play_source):
        if site_play_source['episode_list']:
            return site_play_source['episode_list'][-1]['episode_number']
        return 0

    @staticmethod
    def get_positive_latest_episode_number(site_play_source):
        if site_play_source['episode_list'] and 'type' in site_play_source and site_play_source['type'] == 1:
            return site_play_source['episode_list'][-1]['episode_number']
        return 0

    @staticmethod
    def is_all_episode_not_teg(site_play_source):
        return True

if __name__ == "__main__":
    pl = PlaySourceReader()
    play_source_json = pl.read(sys.argv[1])
    print json.dumps(play_source_json, ensure_ascii=False, encoding='utf8', indent=4)
