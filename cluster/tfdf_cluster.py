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
    for i in range(len(sentences)):
        for j in range(i + 1, len(sentences)):
            prefix = get_prefix_header_key(sentences[i], sentences[j])
            if len(prefix) < 2:
                continue
            flag = True
            for k in header_keys:
                if prefix in k:
                    flag = False
            if flag:
                header_keys.add(prefix.lower().strip())

    for key in header_keys:
        for sen in sentences:
            if key in sen:
                if df_words.has_key(key):
                    df_words[key] += 1
                else:
                    df_words[key] = 1

    return header_keys

sentences, oral_sentences = read_sentences()
sentences_cut_words, df_words, all_cut_words = get_cut_words(sentences)
header_keys = get_header_key(sentences, df_words)

#print 'oral sentences: '
#print '\n'.join(oral_sentences)
#print ''
#print 'sentences: '
#print '\n'.join(sentences)
#print ''
#print 'all cut words: ', '/'.join(all_cut_words)
#print 'header_keys: ', '/'.join(header_keys)
#print 'df_words', ', '.join("%s = %s" % (k, v) for k, v in df_words.items())
#print ''

corpus = []
sen_index = 0
for words in sentences_cut_words:
    line = ' '.join(words)
    #print 'line: ', line
    corpus.append(line)

vectorizer = CountVectorizer()
word_frequence = vectorizer.fit_transform(corpus).toarray()

#print 'sk_learn_keys: ', '/'.join(vectorizer.get_feature_names())
#print 'before: ', word_frequence

# 对词频中每个元素均乘以相应的df
keys = vectorizer.get_feature_names()
for col in range(len(keys)):
    for row in range(len(word_frequence)):
        if df_words.has_key(keys[col]):
            word_frequence[row][col] *= df_words[keys[col]]

#print 'later: ', word_frequence


#transformer = TfidfTransformer()
#weight = transformer.fit_transform(word_frequence).toarray()
#print weight

cluster_num = len(header_keys) / 5
print 'cluster num=%s' % cluster_num
kmeans = KMeans(n_clusters=cluster_num)
#kmeans.fit(weight)
kmeans.fit(word_frequence)
res = kmeans.labels_

stacks = [set() for i in range(cluster_num)]
for i in range(len(sentences)):
    stacks[res[i]].add(i)

cnt = 0
for s in stacks:
    print '\nstack %s' % cnt
    cnt += 1
    for i in s:
        print oral_sentences[i]
