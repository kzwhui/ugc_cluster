#!/usr/bin/python
#encoding=utf8
# just for study crawler, by zwh
# this will crawl top 250 of book of douban in detail
# url: https://book.douban.com/top250

#CREATE TABLE `t_book_info` (
#  `c_id` int(10) unsigned NOT NULL AUTO_INCREMENT,
#  `c_book_title` varchar(256) NOT NULL,
#  `c_book_url` varchar(512) NOT NULL,
#  `c_publish_time` varchar(32) NOT NULL,
#  `c_writer` varchar(512) NOT NULL,
#  `c_price` float NOT NULL,
#  `c_douban_id` varchar(64) NOT NULL,
#  `c_score` float NOT NULL,
#  `c_description` text NOT NULL,
#  `c_modify_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
#  `c_create_time` datetime NOT NULL,
#  PRIMARY KEY (`c_id`),
#  UNIQUE KEY `douban_id` (`c_douban_id`),
#  KEY `book_title` (`c_book_title`),
#  KEY `modify_time` (`c_modify_time`),
#  KEY `create_time` (`c_create_time`)
#) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8

import sys
reload(sys)
sys.setdefaultencoding('utf8') 
sys.path.append('../common')
import re
import requests
import MySQLdb
import json
import traceback
from bs4 import BeautifulSoup as BS
from urlparse import urljoin
from util import *
from common import *
from db_wrapper import *

def transform_value(value):
    if not value:
        return ''

    if isinstance(value, unicode):
        return MySQLdb.escape_string(value).encode("utf8")
    else:
        return MySQLdb.escape_string('%s' % value).encode("utf8")

class DBCrawlerWrapper:
    T_KEYS = ['c_ugc_video_id', 'c_title', 'c_play_site_id', 'c_channel_id', 'c_play_url', 'c_poster_url', 'c_episode', 'c_uploader',
            'c_uploader_url', 'c_upload_time', 'c_tag', 'c_play_amount', 'c_duration', 'c_resolution', 'c_description', 'c_title_s', 'c_media_id']

    def __init__(self):
        db_conf = {
                'host' : '127.0.0.1',
                'user' : 'root',
                'passwd' : 'zheng',
                'db' : 'd_ugc_video',
                'charset' : 'utf8'
                }
        self.db = DBWrapper(db_conf)

    def save_item_info(self, item_list):
        if len(item_list) == 0:
            return True

        sql = "insert into t_ugc_video("
        for key in self.T_KEYS:
            sql += key + ','
        sql += 'c_ctime) values'

        for item in item_list:
            sql += "("
            for key in self.T_KEYS:
                sql += "'%s'," % transform_value(item.get(key, ''))
            sql += 'now()),'

        sql = sql[:-1]
        sql += 'ON DUPLICATE KEY UPDATE c_id = values(c_id)'

        affect = self.db.execute_sql(sql)

        return affect > 0

class Crawler:
    def __init__(self, url, user_name):
        self.base = url
        self.video_list = []
        self.db_crawler_wrapper = DBCrawlerWrapper()
        self.user_name = user_name

    def run(self):
        page_num = 0
        while True:
            resp = requests.get(self.base % page_num)
            json_string = resp.content.split('QZOutputJson=')[1]
            json_string = json_string[:-1]
            data = json.loads(json_string)
            self.parse_data(data)
            
            if not data.has_key('videolst') or not data['videolst']:
                break

            if (page_num * 24 > data['vtotal']):
                break
            else:
                page_num += 1

            self.db_crawler_wrapper.save_item_info(self.video_list)
            self.videolst = []
            print 'page num = %s' % page_num

    def parse_data(self, data):
        for item in data['videolst']:
            info = {}
            info['c_title'] = item['title']
            info['c_title_s'] = item['title_s']
            info['c_play_site_id'] = '12'
            info['c_channel_id'] = '6'
            info['c_play_url'] = item['url']
            info['c_poster_url'] = item['pic']
            info['c_uploader'] = self.user_name
            info['c_upload_time'] = item['uploadtime']
            info['c_media_id'] = item['vid']
            info['c_duration'] = item['duration']
            info['c_description'] = item['desc']
            info['c_ugc_video_id'] = '%s' % get_md5_int(item['url'])
            self.video_list.append(info)

def main():
    #url = 'http://c.v.qq.com/vchannelinfo?otype=json&uin=ee77ecbf17ffd0d667d5bd9617482a49&qm=1&num=24&sorttype=0&orderflag=0&pagenum=%s'
    #user_name = '德古拉Dracula'
    #url = 'http://c.v.qq.com/vchannelinfo?otype=json&uin=5110f96d9fe4d080a7f358a3c1cdb434&qm=1&pagenum=%s&num=24&sorttype=0&orderflag=0'
    #user_name = '瓦洛兰大陆'
    url = "http://c.v.qq.com/vchannelinfo?otype=json&uin=344577a3a62ef0968cd6e6189ae90664&qm=1&pagenum=%s&num=24&sorttype=0&orderflag=0"
    user_name = '魔哒'
    crawler = Crawler(url, user_name)
    crawler.run()

if __name__ == '__main__':
    main()
