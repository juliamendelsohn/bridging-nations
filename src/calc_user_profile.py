import os
import sys
import matplotlib
import ujson
import glob
import pandas as pd
import operator
from tqdm import tqdm
import csv

relevant_users = {}

l1 = sys.argv[1]
l2 = sys.argv[2]

country_1 = sys.argv[3]
country_2 = sys.argv[4]

tweet_files = glob.glob(
    '/shared/0/projects/cross-lingual-exchange/data/relevant_tweets/' + country_1 + '/*.json')
tweet_files.extend(glob.glob(
    '/shared/0/projects/cross-lingual-exchange/data/relevant_tweets/' + country_2 + '/*.json'))

with open('/shared/0/projects/cross-lingual-exchange/data/user_lists/' + country_1 + '.tsv', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    next(reader)
    for row in reader:
        if len(row) == 3:
            relevant_users[row[1].strip()] = {'country': row[2].strip(),
                                              'tweets_in_l1': 0, 'tweets_in_l2': 0, 'bilinguality': -1, 'num_tweets': 0, 'followers': 0,
                                              'following': 0, 'hashtag_count': 0,
                                              'url_count': 0}
        else:
            relevant_users[row[0].strip()] = {'country': row[1].strip(),
                                              'tweets_in_l1': 0, 'tweets_in_l2': 0, 'bilinguality': -1, 'num_tweets': 0, 'followers': 0,
                                              'following': 0, 'hashtag_count': 0,
                                              'url_count': 0}


with open('/shared/0/projects/cross-lingual-exchange/data/user_lists/' + country_2 + '.tsv', 'r') as f:
    reader = csv.reader(f, delimiter='\t')
    next(reader)
    for row in reader:
        if len(row) == 3:
            relevant_users[row[1].strip()] = {'country': row[2].strip(),
                                              'tweets_in_l1': 0, 'tweets_in_l2': 0, 'bilinguality': -1, 'num_tweets': 0, 'followers': 0,
                                              'following': 0, 'hashtag_count': 0,
                                              'url_count': 0}

        else:
            relevant_users[row[0].strip()] = {'country': row[1].strip(),
                                              'tweets_in_l1': 0, 'tweets_in_l2': 0, 'bilinguality': -1, 'num_tweets': 0, 'followers': 0,
                                              'following': 0, 'hashtag_count': 0,
                                              'url_count': 0}


# print(tweet_files)
for infile in tqdm(tweet_files):
    with open(infile, 'r') as f:
        try:
            all_tweets = ujson.load(f)

            for user, tweets in all_tweets.items():

                for tweet in tweets:
                    if tweet['lang'] == l1 and user in relevant_users and tweet['text'][0:2] != 'RT':
                        relevant_users[user]['num_tweets'] += 1
                        relevant_users[user]['tweets_in_l1'] += 1
                        relevant_users[user]['bilinguality'] = relevant_users[user]['tweets_in_l1'] / \
                            relevant_users[user]['num_tweets']
                        relevant_users[user]['hashtag_count'] += len(
                            tweet['entities']['hashtags'])
                        relevant_users[user]['url_count'] += len(
                            tweet['entities']['urls'])
                        relevant_users[user]['followers'] = tweet['user']['followers_count']
                        relevant_users[user]['following'] = tweet['user']['friends_count']

                    if tweet['lang'] == l2 and user in relevant_users and tweet['text'][0:2] != 'RT':
                        relevant_users[user]['num_tweets'] += 1
                        relevant_users[user]['tweets_in_l2'] += 1
                        relevant_users[user]['bilinguality'] = relevant_users[user]['tweets_in_l1'] / \
                            relevant_users[user]['num_tweets']
                        relevant_users[user]['hashtag_count'] += len(
                            tweet['entities']['hashtags'])
                        relevant_users[user]['url_count'] += len(
                            tweet['entities']['urls'])
                        relevant_users[user]['followers'] = tweet['user']['followers_count']
                        relevant_users[user]['following'] = tweet['user']['friends_count']
        except ValueError:
            print('error')
            continue


with open('/shared/0/projects/cross-lingual-exchange/data/temp/relevant_users_with_lang.json', 'w+') as f:
    f.write(ujson.dumps(relevant_users, indent=4))
