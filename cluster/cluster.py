#encoding=utf8

import sys
sys.path.append('../common/')
#from util import *
from db_wrapper import *
import jieba
import jieba.analyse
import collections

def is_not_digit(num):
    try:
        float(num)
        return False
    except:
        return True

db_conf = {
        'host' : '127.0.0.1',
        'user' : 'root',
        'passwd' : 'zheng',
        'db' : 'd_ugc_video',
        'charset' : 'utf8'
        }
db_conn = DBWrapper(db_conf)

user_to_sentences = collections.defaultdict(list)

f = open('../dump/video_info.txt', 'r')
f.readline()
while True:
    line = f.readline()
    if not line:
        break

    items = line.split('\t')
    if len(items) < 7:
        continue
    user_to_sentences[items[6]].append(items[0])

print 'sen num=%s' % len(user_to_sentences)

user_name = '德古拉Dracula'
keywords = []
key_set = set()
jieba.analyse.set_stop_words('/home/zheng/tmp/jieba/extra_dict/stop_words.txt')
for sentence in user_to_sentences[user_name]:
    keys = jieba.analyse.extract_tags(sentence, topK=10)
    keys = filter(is_not_digit, keys)
    keywords.append(keys)
    key_set |= set(keys)

#for i in range(0, 10):
#    print user_to_sentences[user_name][i]
#    print ', '.join(keywords[i])

print 'key num=', len(key_set)
print 'key set: ',  ', '.join(key_set)
