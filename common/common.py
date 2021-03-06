#encoding=utf8
import sys
import datetime
import hashlib
sys.path.append('/usr/local/zk_agent/names/')
#from nameapi import getHostByKey

def get_bigrams(title):
    title = title.replace('-', '')
    title = title.replace(' ', '')
    if len(title) == 1:
        return set(title)
    bigrams = set()
    for i in range(1, len(title)):
        bigram = title[i-1:i+1].strip()
        if not bigram:
            continue
        bigrams.add(bigram)
    return bigrams

#def object_to_string(obj):
#    return json.dumps(obj.__dict__, encoding="UTF-8", ensure_ascii=False)
#
#def dict_to_string(dict):
#    return json.dumps(dict, encoding="UTF-8", ensure_ascii=False)

def rs_hash(key):
    a = 63689
    b = 378551
    hash = 0
    for i in range(len(key)):
        hash = hash * a + ord(key[i])
        a = a * b
        hash = hash % (2**32)
    return hash & 0x7FFFFFFF

# 改进的32位FNV算法1, 综合区导出数据计算表的位置，计算db的位置用上面的算法
def FNVHash1(key):
    p = 16777619
    hash = 2166136261
    for i in range(len(key)):
        hash = (hash ^ ord(key[i])) * p
        hash = hash % (2**32)

    hash += hash << 13
    hash = hash % (2**32)
    hash ^= hash >> 7
    hash = hash % (2**32)
    hash += hash << 3
    hash = hash % (2**32)
    hash ^= hash >> 17
    hash = hash % (2**32)
    hash += hash << 5
    hash = hash % (2**32)
    return hash & 0x7FFFFFFF

def __get_table_number(id, table_count):
    string_id = "%s" % id
    hash_number = rs_hash(string_id)
    return hash_number % table_count

def get_table_number_dict(id_list, count = 200):
    table_number_dict = {}
    for id in id_list:
        table_number = __get_table_number(id,count)
        table_number_dict.setdefault(table_number, [])
        table_number_dict[table_number].append(id)
    return table_number_dict

def get_table_number_dict_for_obj(obj_list, count = 200):
    table_number_dict = {}
    for o in obj_list:
        id = o.MD
        table_number = __get_table_number(id,count)
        table_number_dict.setdefault(table_number, [])
        table_number_dict[table_number].append(o)
    return table_number_dict


def attemp_get_host_from_zk(host, port):
    real_host = host
    real_port = port
    ret, ip, port = getHostByKey(host)
    if ret == 0:
        real_host = ip
        real_port = port
    return (real_host, real_port)

def get_md5_int(calc_str):
    hex_val = hashlib.md5(calc_str.encode('utf8')).hexdigest()
    deci_val = 0 
    for i in range(0,8):
        p = 7-i 
        deci_val = deci_val*256 + int(hex_val[p*2:p*2+2],16)
    return deci_val

def log_str(id):
    now = datetime.datetime.now()
    str_now = now.strftime('%Y-%m-%d %H:%M:%S')
    str_log = "|%s|%s|%s|" % (str_now, str(id), sys.argv[0])
    return str_log
