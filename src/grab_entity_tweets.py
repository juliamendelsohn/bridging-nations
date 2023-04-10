import ujson
import sys
import operator
import os
from collections import *
import glob
import pandas as pd
import re
import networkx as nx


entity = sys.argv[1]
tweet_directory = sys.argv[2]

os.chdir(tweet_directory)


print(entity)

tweet_list = []

for file in glob.glob('*.json'):
    with open(file, 'r') as f:
        try:
            tweet_data = ujson.load(f)
            for user, tweets in tweet_data.items():
                for tweet in tweets:
                    if tweet['text'][0:2] != 'RT' and entity in tweet['entities']:
                        tweet_list.append(tweet['text'])
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(str(e))
            pass

try:
    with open('/shared/1/projects/cross-lingual-exchange/data/named_entities/UK_DE/entity_tweets/' + entity + '_tweets.json', 'w+') as f:
        for tweet in tweet_list:
            f.write(ujson.dumps(tweet))
            f.write('\n')
except KeyboardInterrupt:
    raise
except Exception:
    pass
