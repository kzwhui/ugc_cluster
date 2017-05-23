#!/usr/bin/python
#encoding=utf8
import re
import os 
import pickle
import sys
import getopt
import json
import time,datetime
import logging
import traceback
sys.path.append('../common')
sys.path.append('../conf')
sys.path.append('/usr/local/zk_agent/names/')
reload(sys) 
sys.setdefaultencoding('utf-8')
from log import logger
from nameapi import getHostByKey
from config import g_conf
import common 
import kafka

# protobuf v3
sys.path.append('../pylib/protobuf3.zip')
import vsearch_message_pb2 
import vsearch_sync_message_pb2
from pbjson import dict2pb
# quality/freshness
from get_quality import get_quality
from get_freshness import get_create_time, get_update_time

debug_flag = 0

class CKafkaProtocol(object):
    TOPIC = None
    def __init__(self):
        self.http_conf = g_conf.KAFKA_CONF
        self.TOPIC = g_conf.KAFKA_CONF['topic']
        pass

    def _get_title_md5(self, obj):
        if obj.IF in [1,9,17,23,14]:
            title = obj.TF
        elif obj.IF in [2,16, 5]:
            title = obj.TC
        else:
            title = obj.TA
        md5 = common.get_md5_int(title)
        return md5 & 0xFFFFFFFF

    def add_medias_to_storage(self, media_obj_dict, shield_id_set):
        logger.debug("enter add_medias_to_storage, [%d:%d]", len(media_obj_dict.keys()), len(shield_id_set))
        ignore_id_set = set()
        err_msg = ''
        if not media_obj_dict:
            return True, "no objs to store", ignore_id_set 

        ip_dict = {}
        #这里是一次同步所有数据，如果希望一次同步一个数据，估计不要这样取所有ip
        for i in range (0, 1):
            ip, port = self._get_ip_port()
            ip_dict[ip] = "%s:%s"

        ip_port = None
        for ip, s in ip_dict.items():
            if ip_port: 
                ip_port = "%s,%s:%s" % (ip_port, ip, port)
            else:
                ip_port = "%s:%s" % (ip, port)

        if not ip_port:
            err_msg = 'no kafka-ip/port'
            logger.error("%s" % err_msg) 
            return False, err_msg, media_obj_dict.keys()
        logger.debug("ip_ports=%s" % ip_port) 

        producer = None
        try:
            producer = kafka.KafkaProducer(bootstrap_servers=[ip_port],retries=3,linger_ms=0,max_block_ms=0)
        except Exception,e:
            logger.error(traceback.format_exc())
            logger.error("err, connect kafka failed!")
            return False, err_msg, media_obj_dict.keys()

        now = datetime.datetime.now()
        for md, o in media_obj_dict.items():
            cur_timestamp = int(time.time())
            prod = CKafkaProtocol()
            message = {
                'header': {
                    'topic':self.TOPIC,
                    'timestamp': int(time.time()),
                    'key': o.get_id(),
                },
                'action': vsearch_message_pb2.Message.ADD,#add/delete
            }

            body_dict = {
                "MD" : md
            }

            for f in vsearch_sync_message_pb2.SyncBody.DESCRIPTOR.fields:
                field = f.name
                #类型https://developers.google.com/protocol-buffers/docs/reference/python/google.protobuf.descriptor.FieldDescriptor-class
                if f.type < 1 or f.type >= 18:
                    logger.error("name=[%s] invalid type=[%s] " % (field, f.type))
                    continue
                if hasattr(o, field) and o.__getattribute__(field) is not None:
                    val = o.__getattribute__(field)
                    try:
                        val = self._type_transform(val, f.type)
                    except Exception,e:
                        logger.error(traceback.format_exc())
                        logger.error("_type_transform, val=[%s] type=[%s] field=[%s] " % (o.__getattribute__(field), f.type, field))
                        continue

                    if val is None:
                        continue
                    body_dict[field] = val 

            # 计算quality和freshness
            quality, _ = get_quality(o)
            CREATE, _, _ = get_create_time(o, now)
            if not o.IF in [1, 23]:
                UPDATE, _, _ = get_update_time(o, now, with_extra=True)
            else:
                UPDATE = CREATE
            body_dict['WE'] = quality
            body_dict['DM'] = int(time.mktime(CREATE.timetuple())) + 8*3600
            body_dict['DN'] = int(time.mktime(UPDATE.timetuple())) + 8*3600

            # 增加IN字段
            body_dict['IN'] = '%s' % self._get_title_md5(o)

            message['sync_body'] = body_dict
            if 1 == debug_flag: 
                print "\nbody_dict:[%s]\n" % md
                for k, v in body_dict.items():
                    print "%s:%s" % (k, v)

                print "\n"

            if not self._write_to_kafka(producer, message, bytes(o.get_id())):
                ignore_id_set.add(o.get_id())
                logger.error("err, md=[%s] body_dict={\n%s\n}" % (md, '\n' . join(["%s:%s" % (k, v) for k, v in body_dict.items()])))

        if shield_id_set:
            for media_id in shield_id_set:
                md = self._get_MD(media_id)
                cur_timestamp = int(time.time())
                prod = CKafkaProtocol()
                message = {
                    'header': {
                        'topic':self.TOPIC,
                        'timestamp': int(time.time()),
                        'key': media_id,
                    },
                    'action': vsearch_message_pb2.Message.DELETE,#add/delete
                }

                body_dict = {
                    "MD" : md
                }
                message['sync_body'] = body_dict
                if 1 == debug_flag: 
                    print "\nbody_dict:[%s]\n" % md
                    for k, v in body_dict.items():
                        print "%s:%s" % (k, v)

                    print "\n"

                if not self._write_to_kafka(producer, message, bytes(o.get_id())):
                    ignore_id_set.add(o.get_id())
                    logger.error("err, md=[%s] body_dict={\n%s\n}" % (md, '\n' . join(["%s:%s" % (k, v) for k, v in body_dict.items()])))

        producer.flush(30)
        logger.debug("producer.send=[%s] [%s] " % (self.TOPIC, md))
        producer.close(20)


        if not ignore_id_set :
            return True, '', ignore_id_set 

        return False, 'err', ignore_id_set 


    def _write_to_kafka(self, producer, message, route_key):
        try:
            msg = dict2pb(vsearch_message_pb2.Message, message, strict=True)

            # print Message
            pb_buffer = msg.SerializeToString()
            if 1 == debug_flag: 
                print ">> Gen message len =", len(pb_buffer)
                msg2 = vsearch_message_pb2.Message()
                msg2.ParseFromString(pb_buffer)
                print ">> decode message:"
                print msg2

            future = producer.send(topic=self.TOPIC, key=route_key, value=pb_buffer)
            try:
                future.get()
                if not future.is_done:
                    logger.error("write to kafka err, route_key=[%s] " % (route_key)) 
                    return False

            except Exception,e:
                logger.error(traceback.format_exc())
                logger.error("write to kafka err, route_key=[%s]" % (md))
                return False

            if 1 == debug_flag:
                print "%s\n%s" % (future, future.__dict__)

        except Exception,e:
            print("[ERROR] write_to_kafka(%s) failed: %s" % (route_key, e))
            logger.error(traceback.format_exc())
            logger.error("err, route_key=[%s]" % (route_key))
            return False
        return True 


    def _get_ip_port(self):
        ret, ip, port = getHostByKey(self.http_conf['host'])
        if not ip or not port:
            logger.debug("getHostByKey failed:[%s] [%s] [%s] [%s]" % (ret, ip, port, self.http_conf['host']))
            ip = self.http_conf['host']
            port = self.http_conf['port']
            logger.debug("getHostByKey:[%s] [%s] [%s]" % (ret, ip, port))

        return ip, port

    def _type_transform(self, val, type_id):
        if type(val) is list: 
            new_val = []
            for v in val:
                nv = self._type_transform(v, type_id)
                if nv and nv is not 0:
                    new_val.append(nv)

            return new_val

        if not val and (val is not 0 or val is not '0'):
            logger.debug("igore val:[%s] [%d] " % (val, type_id))
            return None

        if type_id < 3:
            return float(val)
        if type_id < 8 or type_id == 13:
            return int(val)
        if type_id == 9:
            return str(val)

        logger.error("fail to transform type:[%s] [%d] " % (val, type_id))
        return None

    def _get_MD(self, media_id):
        cmd = "../common/getmd5 -i %s" % media_id
        res = os.popen(cmd).read()
        res = res.strip("\n")
        logger.info("res={%s}" % res)
        match_re_obj = re.match(r'^.*?md5\s+is\s+(\d+)$', res)
        if match_re_obj:
            md = match_re_obj.group(1)
            return md
        else:
            raise Exception("no MD caculated:[%s] ", media_id)
        return None


def test_kafka():
    prod = CKafkaProtocol()
    #prod.add_medias_to_storage(None)

    message = {
        'header': {
            'topic': 'mugc',
            'timestamp': int(time.time()),
        },
        'action': vsearch_message_pb2.Message.UPDATE,
    }

    body = {
        'MD': '14121047840296143732',
        'IB': '0dfpyvfa7tp0ewe',
        'WF': 0.0,
    }
    message['sync_body'] = body

    msg = dict2pb(vsearch_message_pb2.Message, message, strict=True)

    # print Message
    pb_buffer = msg.SerializeToString()
    print ">> Gen message len =", len(pb_buffer)

    msg2 = vsearch_message_pb2.Message()
    msg2.ParseFromString(pb_buffer)

    print ">> decode message:"
    print msg2





def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:", [ "--id"])
    except getopt.GetoptError as err:
        # print help information and exit:
        print str(err) # will print something like "option -a not recognized"
        usage()
        sys.exit(2)

    test_kafka()
    # _get_fields_for_test()
    # mapTabidInfo = get_tabid_map()
    # mapTypeToConfig = get_quality_config()
    # print "\nmapTabidInfo=\n"
    # for k, v in mapTabidInfo.items():
    #     print "k=%s, v=%s" % (k, v)

    # print "\nmapTypeToConfig=\n"
    # for k, v in mapTypeToConfig.items():
    #     print "k=%s, v=%s" % (k, v)


if __name__ == '__main__':
    #test_redis_writer()
    main()

