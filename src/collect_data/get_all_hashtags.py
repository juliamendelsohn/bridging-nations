import glob
import gzip
import os
import csv
import ujson as json
from collections import defaultdict,Counter
import pandas as pd


def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())


def get_hashtags_from_tweet_obj(tweet):
    hashtags = set([h['text'].lower() for h in tweet['entities']['hashtags']])
    hashtags_extended = set()
    if 'extended_tweet' in tweet:
        hashtags_extended = set([h['text'].lower() for h in tweet['extended_tweet']['entities']['hashtags']])
    hashtags = hashtags.union(hashtags_extended)
    return list(hashtags)


def get_tweets_with_hashtags(filename,country_hashtags,language_hashtags,include_rt=False):
    with gzip.open(filename,'r') as f:
        for i,line in enumerate(f):
            tweet = load_tweet_obj(line)
            if include_rt or not tweet['text'].startswith('RT'):
                country = tweet['user']['country']
                language = tweet['lang']
                if country not in country_hashtags:
                    country_hashtags[country] = Counter()
                if language not in language_hashtags:
                    language_hashtags[language] = Counter()
                hashtags = get_hashtags_from_tweet_obj(tweet)
                for hashtag in hashtags:
                    country_hashtags[country][hashtag] += 1
                    language_hashtags[language][hashtag] += 1
                if i % 10000 == 0:
                    print(filename,i)

def write_entries(out_dir,hashtag_dict):
    for key in hashtag_dict:
        out_file = os.path.join(out_dir,key + '.json')
        with open(out_file,'w') as f:
            json.dump(hashtag_dict[key],f)


def main():

    tweet_dir = "/shared/2/projects/cross-lingual-exchange/data/relevant_tweets/all"
    out_dir = "/shared/2/projects/cross-lingual-exchange/data/hashtags_no_rt/"
    country_out_dir = os.path.join(out_dir,'country')
    language_out_dir = os.path.join(out_dir,'language')
    for out_path in [out_dir,country_out_dir,language_out_dir]:
        if not os.path.exists(out_path):
            os.mkdir(out_path)

    country_hashtags = {}
    language_hashtags = {}
    all_tweet_files = glob.glob(os.path.join(tweet_dir,'*.gz'))
    for filename in all_tweet_files:
        get_tweets_with_hashtags(filename,country_hashtags,language_hashtags)
    
    print("writing country entries")
    write_entries(country_out_dir,country_hashtags)

    print("writing language entries")
    write_entries(language_out_dir,language_hashtags)
    
    



    


if __name__ == "__main__":
    main()
