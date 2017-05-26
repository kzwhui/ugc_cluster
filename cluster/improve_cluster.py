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
from sklearn.cluster import KMeans
from union_find import *

def is_not_digit(num):
    try:
        float(num)
        return False
    except:
        return True

def read_sentences():
    sentences = open('data.txt', 'r').read().split('\n')
    sentences = filter(lambda s: s, sentences)
    return sentences

def get_key_words(sentences):
    jieba.analyse.set_stop_words('../extra_words/stop_words.txt')
    sentence_keys = []
    all_keys = set()
    for sentence in sentences:
        tags = jieba.analyse.extract_tags(sentence, topK = 10)
        tags = filter(is_not_digit, tags)
        sentence_keys.append(tags)
        all_keys |= set(tags)

    return all_keys, sentence_keys

def count_word_frequence(corpus):
    vectorizer = CountVectorizer(min_df=1)
    X = vectorizer.fit_transform(corpus)
    return vectorizer.get_feature_names(), X.toarray()

def count_cosine(array):
    row_len = len(array)
    col_len = len(array[0])
    ans = []
    for i in range(row_len):
        sub_ans = [0 for i in range(0, row_len)]
        ans.append(sub_ans)
        
    for i in range(0, row_len):
        for j in range(i + 1, row_len):
            a = 0
            b = 0
            c = 0
            for col in range(0, col_len):
                a += array[i][col] * array[j][col]
                b += array[i][col] * array[i][col]
                c += array[j][col] * array[j][col]

            cosine = 0
            if (b + c) > 0:
                cosine = 1.0 * a / (math.sqrt(b) * math.sqrt(c))
            ans[i][j] = ans[j][i] = cosine

    return ans

def get_classify(sentences, similarity_array, lowest_similarity):
    print 'lowest_similarity=', lowest_similarity
    similarity_tag = lowest_similarity
    classified_barrel = collections.defaultdict(set)

    for i in range(0, len(sentences)):
        for j in range(i + 1, len(sentences)):
            if similarity_array[i][j] >= similarity_tag and (find(i) != find(j)):
                union(i, j)

    for i in range(0, len(sentences)):
        root = find(i)
        classified_barrel[root].add(root)
        classified_barrel[root].add(i)

    return classified_barrel

def remove_apostrophe(s):
    exclude = set(string.punctuation)
    exclude |= set(punctuation)
    s = ''.join(ch if ch not in exclude else ' ' for ch in s.decode('utf8'))
    return s

def get_prefix_header_key(left_string, right_string):
    left_string = left_string.decode('utf8')
    right_string = right_string.decode('utf8')
    i = 0
    for i in range(0, min(len(left_string), len(right_string))):
        if (left_string[i] != right_string[i]):
            break

    return left_string[:i]

def get_header_key(sentences):
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
                header_keys.add(prefix)

    return header_keys

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

def get_topn_keys(sentences_cut_words, df_words, topn):
    print 'topn, num=%s' % topn
    sentences_top_keys = []
    for words in sentences_cut_words:
        words_score = []
        for w in words:
            wd = {}
            wd['name'] = w
            wd['score'] = df_words[w]
            words_score.append(wd)

        words_score = sorted(words_score, key=lambda x: x['score'], reverse=True)

        top_keys = set()
        for i in range(0, min(topn, len(words_score))):
            top_keys.add(words_score[i]['name'])
        sentences_top_keys.append(top_keys)

    return sentences_top_keys

sentences = read_sentences()
sentences_cut_words, df_words, all_cut_words = get_cut_words(sentences)
print 'all_cut_words, num=', len(all_cut_words)
#print 'each sentence keys'
#for cut_words in sentences_cut_words:
#    print '/'.join(cut_words)
#    print ''

#print 'each key, doc num'
#for k, v in df_words.items():
#    print 'key=%s, doc num=%s' % (k, v)

each_sentence_top_key_num = 0
for keys in sentences_cut_words:
    each_sentence_top_key_num += len(keys)
sentences_top_keys = get_topn_keys(sentences_cut_words, df_words, each_sentence_top_key_num / len(sentences_cut_words))
#print 'each sentence top key'
#for keys in sentences_top_keys:
#    print '/'.join(keys)

header_keys = get_header_key(sentences)
print 'header keys: ', '/'.join(header_keys)
for i in range(len(sentences_top_keys)):
    for k in header_keys:
        if k in sentences[i]:
            sentences_top_keys[i].add(k)

cut_sentences_list = []
for keys in sentences_top_keys:
    line = ' '.join(keys)
#    print 'sk line: ', line
    cut_sentences_list.append(line)

sk_learn_keys, sk_learn_array = count_word_frequence(cut_sentences_list)
print 'sk leanr keys, num=', len(sk_learn_keys)
#print 'sk learn keys: ', ', '.join(sk_learn_keys)
#print 'sk learn array:'
#for ar in sk_learn_array:
#    print ', '.join('%s' % x for x in ar)

#cos_ans = count_cosine(sk_learn_array)
#print 'cosine'
#for i in range(0, len(cos_ans)):
#    for j in range(i + 1, len(cos_ans)):
#        print sentences[i], '\t --- \t', sentences[j], '\t cos = \t', cos_ans[i][j]

#lowest_similarity = 1.0 * len(sk_learn_keys) / len(all_cut_words)
#classified_barrel = get_classify(sentences, cos_ans, lowest_similarity)
#cnt = 1
#for k, v in classified_barrel.items():
#    print '\nbarrel %s:' % cnt
#    cnt += 1
#    print '\n'.join([sentences[i] for i in v])
#    print ''

# 使用k means均值算法
print 'header key num=%s' % len(header_keys)
cluster_num = 15 
kmeans = KMeans(n_clusters=cluster_num, random_state=0)
kmeans.fit(sk_learn_array)
res = kmeans.predict(sk_learn_array)

#print res

stacks = [set() for i in range(cluster_num)]
for i in range(len(sentences)):
    stacks[res[i]].add(i)

#cnt = 0
#for s in stacks:
#    print 'num: %s' % cnt 
#    cnt += 1
#    print s
cnt = 1
for s in stacks:
    print 'barrier %s' % cnt
    cnt += 1
    for i in s:
        print sentences[i]
    print ''
