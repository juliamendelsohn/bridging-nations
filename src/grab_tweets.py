import sys
import ujson
import os
import json
import bz2
import glob
from tqdm import tqdm
import spacy
import csv

tweet_file = sys.argv[1]

acceptable_langs = set()
relevant_tweets = {}
name_to_code = {}

processed_files = set()

with open('/home/sayghosh/cross-lingual-exchange/data/relevant_languages.txt', 'r') as f:
    for line in f:
        acceptable_langs.add(line.strip())

with open('/home/sayghosh/cross-lingual-exchange/data/country_names_to_code.tsv', 'r') as f:
    for line in f:
        name_to_code[line.split()[0].strip()] = line.split()[1].strip() 


for country_file in glob.glob('/shared/0/projects/cross-lingual-exchange/data/user_lists/*.tsv'):
    with open(country_file, 'r') as f:
        reader = csv.reader(f, delimiter='\t')
        next(reader)
        for row in reader:
            if len(row) == 2:
                user_id = row[0].strip()
                country = row[1].strip()
                if country == 'United Kingdom': 
                    country = 'United_Kingdom'
            else:
                user_id = row[1].strip()
                country = row[2].strip()

            if country not in relevant_tweets:
                relevant_tweets[country] = {}

            relevant_tweets[country][user_id] = []


out_file = os.path.splitext(os.path.basename(tweet_file))[0]

if '/shared/0/projects/cross-lingual-exchange/data/relevant_tweets/ES/' + out_file + '.json' not in processed_files:
    with bz2.open(tweet_file) as f:
        print("processing {}".format(out_file))

        for line in f:
            try:
                tweet = ujson.loads(line)
                tweet['named_entities'] = {}

                if (tweet['lang'] in acceptable_langs 
                    and tweet['text'][0:2] != 'RT' and tweet['user']['id_str'] and not tweet['user']['verified']):

                    for country, users in relevant_tweets.items():
                        if tweet['user']['id_str'] in users:
                            relevant_tweets[country][tweet['user']['id_str']].append(tweet)

            except Exception:
                continue

    print("processed {}".format(out_file))
    for country in relevant_tweets:
        with open('/shared/0/projects/cross-lingual-exchange/data/relevant_tweets/' + name_to_code[country] + '/' + out_file + '.json', 'w+') as outf:
            outf.write(ujson.dumps(relevant_tweets[country], indent=4))
