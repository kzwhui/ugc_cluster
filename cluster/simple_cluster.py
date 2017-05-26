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

def read_sentences():
    sentences = open('data.txt', 'r').read().split('\n')
    sentences = filter(lambda s: s, sentences)
    return sentences

def get_cut_words(sentences):
    sentences_cut_words = []
    df_words = {}
    all_cut_words = set()
    stop_words = set(open('../extra_words/stop_words.txt', 'r').read().split())
    for sen in sentences:
        tags = jieba.cut(sen, cut_all = True)
        xtags = filter(lambda x : x and x not in stop_words, tags)
        for k in xtags:
            if df_words.has_key(k):
                df_words[k] += 1
            else:
                df_words[k] = 1
        all_cut_words |= set(xtags)
        sentences_cut_words.append(set(xtags))

    return sentences_cut_words, df_words, all_cut_words

sentences = read_sentences()
sentences_cut_words, df_words, all_cut_words = get_cut_words(sentences)

corpus = []
for words in sentences_cut_words:
    line = ' '.join(words)
    corpus.append(line)

vectorizer = CountVectorizer()
word_frequence = vectorizer.fit_transform(corpus).toarray()
print '/'.join(vectorizer.get_feature_names())
#print word_frequence

transformer = TfidfTransformer()
weight = transformer.fit_transform(word_frequence).toarray()
#print weight

cluster_num = 10
kmeans = KMeans(n_clusters=cluster_num)
#kmeans.fit(weight)
kmeans.fit(word_frequence)
res = kmeans.labels_

stacks = [set() for i in range(cluster_num)]
for i in range(len(sentences)):
    stacks[res[i]].add(i)

cnt = 0
for s in stacks:
    print '\nbarrier %s' % cnt
    cnt += 1
    for i in s:
        print sentences[i]
