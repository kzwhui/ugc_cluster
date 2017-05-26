#encoding=utf8

import sys
reload(sys)
sys.setdefaultencoding('utf8')
sys.path.append('../common/')
from util import *
from db_wrapper import *
import jieba
import re
import jieba.analyse
import collections
import math
import string
from zhon.hanzi import punctuation
from sklearn.feature_extraction.text import CountVectorizer
from union_find import *
from sklearn.cluster import KMeans
from sklearn.feature_extraction.text import TfidfTransformer

def remove_apostrophe(s):
    exclude = set(string.punctuation)
    exclude |= set(punctuation)
    s = ''.join(ch if ch not in exclude else ' ' for ch in s.decode('utf8'))
    return s

def read_sentences():
    oral_sentences = open('data.txt', 'r').read().split('\n')
    oral_sentences = filter(lambda s: s, oral_sentences)
    sentences = map(remove_apostrophe, oral_sentences)
    sentences = map(lambda x: re.sub('\s', '', x), sentences)
    sentences = map(lambda x: re.sub('\d', '', x), sentences)
    sentences = map(lambda x: x.lower(), sentences)
    return sentences, oral_sentences

def get_cut_words(sentences):
    sentences_cut_words = []
    df_words = {}
    all_cut_words = set()
    stop_words = set(open('../extra_words/stop_words.txt', 'r').read().split())
    for sen in sentences:
        tags = jieba.cut(sen, cut_all = True)
        xtags = filter(lambda x : x and x not in stop_words, tags)
        xtags = map(lambda x : x.lower(), xtags)
        for k in xtags:
            if df_words.has_key(k):
                df_words[k] += 1
            else:
                df_words[k] = 1
        all_cut_words |= set(xtags)
        sentences_cut_words.append(set(xtags))

    return sentences_cut_words, df_words, all_cut_words

def get_prefix_header_key(left_string, right_string):
    left_string = left_string.decode('utf8')
    right_string = right_string.decode('utf8')
    i = 0
    for i in range(0, min(len(left_string), len(right_string))):
        if (left_string[i] != right_string[i]):
            break

    return left_string[:i]

def get_header_key(sentences, df_words):
    header_keys = set()
    headers_list = []
    sentences = sorted(sentences)
    for i in range(1, len(sentences)):
        temp = get_prefix_header_key(sentences[i - 1], sentences[i])
        if len(temp) > 3:
            headers_list.append(temp)

    for i in range(len(headers_list)):
        for j in range(i + 1, len(headers_list)):
            if not headers_list[i]:
                break
            if not headers_list[j]:
                continue

            if headers_list[i].startswith(headers_list[j]):
                headers_list[i] = None
                break
            if headers_list[j].startswith(headers_list[i]):
                headers_list[j] = None
                continue

    for h in headers_list:
        if h:
            header_keys.add(h)

    for key in header_keys:
        for sen in sentences:
            if sen.startswith(key):
                if df_words.has_key(key):
                    df_words[key] += 1
                else:
                    df_words[key] = 1

    return header_keys

sentences, oral_sentences = read_sentences()
df_words = {}
header_keys = get_header_key(sentences, df_words)
header_to_ids = collections.defaultdict(set)
for i in range(len(sentences)):
    for head in header_keys:
        if sentences[i].startswith(head):
            header_to_ids[head].add(i)

cnt = 0
for k, v in header_to_ids.items():
    print '\nstack = %s, cid name ---- %s' % (cnt, k)
    print '\n'.join(oral_sentences[i] for i in v)
    cnt += 1
