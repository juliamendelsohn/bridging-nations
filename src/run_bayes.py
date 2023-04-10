#!/usr/local/bin/python
from collections import *
import math
import argparse
import sys

import os.path
# Dan Jurafsky March 22 2013
# bayes.py
# Computes the "weighted log-odds-ratio, informative dirichlet prior" algorithm for
# from page 388 of
# Monroe, Colaresi, and Quinn. 2009. "Fightin' Words: Lexical Feature Selection and Evaluation for Identifying the Content of Political Conflict"


from os import listdir
from os.path import isfile, join, basename
import re
import gzip
import string

import ujson as json


from twokenize import tokenize
import sys
# sys.setdefaultencoding() does not exist, here!
reload(sys)  # Reload does the trick!
sys.setdefaultencoding('UTF8')


def myswap(inlist):
    return [inlist[0], int(inlist[1])]


def load(fname, min_freq):
    c = Counter()
    with open(fname) as f:
        for line in f:
            cols = line[:-1].split('\t')
            freq = int(cols[1])
            # if freq >= min_freq:
            c[cols[0]] += freq

    d = defaultdict(int)
    for k, v in c.iteritems():
        if v > min_freq:
            d[k] = v
    return d


def main():

    input_dir = sys.argv[1]
    output_dir = sys.argv[2]
    tweet_key = 'mentioning_tweets'
    if len(sys.argv) > 3:
        tweet_key = sys.argv[3]
    max_tweets = -1
    if len(sys.argv) > 4:
        max_tweets = int(sys.argv[4])

    max_lines = -1
    if len(sys.argv) > 5:
        max_lines = int(sys.argv[5])

    aggregated_files = [join(input_dir, f)
                        for f in listdir(input_dir) if 'json' in f]

    users_seen = set()
    prior = Counter()

    bigram_prior = Counter()

    fname_to_counts = {}
    fname_to_bigram_counts = {}

    medical_degrees = set(['dds', 'dpt', 'mdphd'])

    for fname in aggregated_files:

        attr_to_counts = defaultdict(Counter)
        attr_to_bigram_counts = defaultdict(Counter)
        fname_to_counts[fname] = attr_to_counts
        fname_to_bigram_counts[fname] = attr_to_bigram_counts

        print fname

        unigram_per_user = Counter()
        bigram_per_user = Counter()

        with open(fname) as f:
            for line_no, line in enumerate(f):
                user_entry = json.loads(line)
                #cols = line.split('\t')
                #username = cols[0]
                username = user_entry['user']
                should_add = not username in users_seen
                users_seen.add(username)

                #attr = cols[1]
                attr = user_entry['attr']

                # a bit of preprocessing
                if 'age' in fname:
                    age = int(attr)
                    if age < 14 or age >= 70:
                        continue
                    attr = '%d0s' % (age/10)
                elif 'religion' in fname:
                    if attr == 'catholic':
                        attr = 'christian'
                    elif attr == 'islamic':
                        attr = 'muslim'
                elif 'degrees' in fname:
                    if attr in medical_degrees:
                        attr = 'medical-doctor'
                    elif attr == 'dphil':
                        attr = 'phd'

                unique_unigrams = set()
                unique_bigrams = set()

                tweet_obj = user_entry[tweet_key]
                if not isinstance(tweet_obj, dict):
                    tweet_obj = {'tmp': tweet_obj}
                for from_user, tweets in tweet_obj.iteritems():

                    if max_tweets > 0:
                        tweets = tweets[:max_tweets]

                    for tweet in tweets:
                        tweet = tweet.lower()
                        tweet = re.sub(r'@[^ ]+', ' ', tweet)
                        tokens = [x.strip(string.punctuation) for x in tweet.split()
                                  if not (x.startswith('http') or x.startswith('pic.'))]
                        #tokens = [x for x in  tokenize(tweet) if  not (x.startswith('http') or x.startswith('pic.'))]
                        unigram_tokens = [x for x in tokens if len(
                            x) >= 3 and re.match(r'[a-z]+', x)]

                        for t in unigram_tokens:
                            unique_unigrams.add(t)

                        attr_to_counts[attr].update(unigram_tokens)
                        if should_add:
                            prior.update(unigram_tokens)

                        for i in range(len(tokens) - 1):
                            #t1 = tokens[i]
                            #t2 = tokens[i+1]

                            bigram = tokens[i] + "_" + tokens[i+1]
                            attr_to_bigram_counts[attr][bigram] += 1
                            bigram_prior[bigram] += 1
                            unique_bigrams.add(bigram)

                for t in unique_unigrams:
                    unigram_per_user[t] += 1
                for t in unique_bigrams:
                    bigram_per_user[t] += 1

                if (line_no + 1) % 100 == 0:
                    print 'Saw %d users, %d unigrams, %d bigrams' % (line_no+1, len(unique_unigrams), len(unique_bigrams))
                    # break

                if max_lines > 0 and (line_no + 1) % max_lines == 0:
                    break

    print 'loaded %d terms across %d users' % (len(prior), len(users_seen))

    valid_unigrams = set()
    for t, c in unigram_per_user.iteritems():
        if c >= 5:
            valid_unigrams.add(t)
    valid_bigrams = set()
    for t, c in bigram_per_user.iteritems():
        if c >= 5:
            valid_bigrams.add(t)

    for fname, attr_to_counts in fname_to_counts.iteritems():
        print 'generating from ', fname
        for attr, counts in attr_to_counts.iteritems():
            rest = Counter()
            for attr2, counts2 in attr_to_counts.iteritems():
                if attr2 != attr:
                    rest.update(counts2)

            bn = basename(fname)

            output_file = output_dir + '/' + \
                bn.split('.')[0] + '.' + attr + '.tsv'
            run(counts, rest, prior, valid_unigrams, output_file)

    for fname, attr_to_bigram_counts in fname_to_bigram_counts.iteritems():
        print 'generating bigrams from ', fname
        for attr, bigram_counts in attr_to_bigram_counts.iteritems():
            rest = Counter()
            for attr2, bigram_counts2 in attr_to_bigram_counts.iteritems():
                if attr2 != attr:
                    rest.update(bigram_counts2)

            bn = basename(fname)

            output_file = output_dir + '/' + \
                bn.split('.')[0] + '.' + attr + '.bigrams.tsv'
            run(bigram_counts, rest, prior, valid_bigrams, output_file)


def run(counts1, counts2, prior, valid_words, output_file):

    min_freq = 1

    print 'Total vocab size: %d; %d for arg1, %d for arg2' % (len(prior), len(counts1), len(counts2))

    sigmasquared = defaultdict(float)
    sigma = defaultdict(float)
    delta = defaultdict(float)

    used_words = set(counts1.iterkeys()) | set(counts2.iterkeys())
    used_words &= set(prior.iterkeys())
    used_words &= valid_words

    for word in prior.iterkeys():
        prior[word] = int(prior[word] + 0.5)

    for word in counts2.iterkeys():
        counts1[word] = int(counts1[word] + 0.5)
        if prior[word] == 0:
            prior[word] = 1

    for word in counts1.iterkeys():
        counts2[word] = int(counts2[word] + 0.5)
        if prior[word] == 0:
            prior[word] = 1

    n1 = sum(counts1.itervalues())
    n2 = sum(counts2.itervalues())
    nprior = sum(prior.itervalues())

    # for word in prior.keys():
    for word in used_words:
        # if prior[word] == 0 and (counts2[word] > 10):
        #prior[word] = 1
        if prior[word] > 0:
            l1 = float(counts1[word] + prior[word]) / \
                ((n1 + nprior) - (counts1[word] + prior[word]))
            l2 = float(counts2[word] + prior[word]) / \
                ((n2 + nprior) - (counts2[word] + prior[word]))
            sigmasquared[word] = 1/(float(counts1[word]) + float(prior[word])) + \
                1/(float(counts2[word]) + float(prior[word]))
            sigma[word] = math.sqrt(sigmasquared[word])
            delta[word] = (math.log(l1) - math.log(l2)) / sigma[word]

    with open(output_file, 'w') as outf:
        for word in sorted(delta, key=delta.get):
            outf.write('%s\t%.3f\n' % (word, delta[word]))


if __name__ == '__main__':
    main()
