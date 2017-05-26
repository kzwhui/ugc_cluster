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
    #jieba.analyse.set_stop_words('../extra_words/stop_words.txt')
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

            cosine = 1.0 * a / (math.sqrt(b) * math.sqrt(c))
            ans[i][j] = ans[j][i] = cosine

    return ans

def get_classify(sentences, similarity_array):
    similarity_tag = 0.15
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

sentences = read_sentences()
#print '\n'.join(sentences)
#print ''

header_keys = get_header_key(sentences)
#print 'header keys: ', '/'.join(header_keys)
#print ''

all_keys, sentence_keys = get_key_words(sentences)
#for keys in sentence_keys:
#    print ', '.join(keys)
#    print ''
#print ''
#print 'key num=', len(all_keys)
#print 'jieba keys = ', ', '.join(all_keys)

cut_sentences_list = []
cnt = 0
for sen in sentences:
    key_set = set()

    # add header key
    #for h_key in header_keys:
    #    if h_key in sen:
    #        key_set.add(h_key)

    #sen = re.sub(r'\w+', ' ', sen)
    #sen = remove_apostrophe(sen)
    tags = jieba.cut(sen)
    tags = filter(is_not_digit, tags)
    #key_set |= set(sen.split('!'))
    #key_set |= set(sen.split('-'))
    #key_set |= set(sen.split())
    #key_set |= set(tags)
    key_set |= set(sentence_keys[cnt])

    #line = ' '.join(tags)  + '' + ' '.join(sentence_keys[cnt])#+ ' ' + sen + ' '.join(sen.split('-'))
    line = ' '.join(key_set)
    #print 'line: ', line
    cnt += 1
    #print ' '.join(tags)
    cut_sentences_list.append(line)
sk_learn_keys, sk_learn_array = count_word_frequence(cut_sentences_list)

transformer = TfidfTransformer()
tfidf = transformer.fit_transform(sk_learn_array)
sk_learn_array = tfidf.toarray()
#print sk_learn_array

#print 'sk learn keys: ', ', '.join(sk_learn_keys)
#print 'sk learn array: ', sk_learn_array

#cos_ans = count_cosine(sk_learn_array)
#print cos_ans
#print 'cosine'
#for i in range(0, len(cos_ans)):
#    for j in range(i + 1, len(cos_ans)):
#        print sentences[i], '\t --- \t', sentences[j], '\t cos = \t', cos_ans[i][j]
#
#classified_barrel = get_classify(sentences, cos_ans)
#cnt = 1
#for k, v in classified_barrel.items():
#    print '\nbarrel %s:' % cnt
#    cnt += 1
#    print '\n'.join([sentences[i] for i in v])
#    print ''


print 'header key num=%s' % len(header_keys)
cluster_num = len(header_keys)
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
cnt = 0
for s in stacks:
    print '\nbarrier %s' % cnt
    cnt += 1
    for i in s:
        print sentences[i]
