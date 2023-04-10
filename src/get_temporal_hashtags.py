import glob
import gzip
import os
import csv
import ujson as json
from collections import defaultdict,Counter
import pandas as pd
from datetime import datetime,timedelta



def separate_files_by_interval(tweet_dir,interval_in_days,year_range,start_date):
    files_by_interval = defaultdict(list)
    for year in year_range:
        for m in range(1,13):
            month = str(m)
            if len(month) == 1:
                month = '0' + month
            for d in range(1,32):
                day = str(d)
                if len(day) == 1:
                    day = '0' + day
                try:
                    date = datetime(year,m,d)
                    interval = int((date - start_date).days / interval_in_days)
                    tweet_files_1 = glob.glob(os.path.join(tweet_dir,f'decahose.{year}-{month}-{day}*.gz'))
                    tweet_files_2 = glob.glob(os.path.join(tweet_dir,f'decahose.{year}{month}{day}*.gz'))
                    new_files = tweet_files_1 + tweet_files_2
                    if len(new_files) > 0:
                        files_by_interval[interval] += new_files
                except:
                    continue
    return files_by_interval


def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())


def get_hashtags_from_tweet_obj(tweet):
    hashtags = set([h['text'].lower() for h in tweet['entities']['hashtags']])
    hashtags_extended = set()
    if 'extended_tweet' in tweet:
        hashtags_extended = set([h['text'].lower() for h in tweet['extended_tweet']['entities']['hashtags']])
    hashtags = hashtags.union(hashtags_extended)
    return list(hashtags)


def get_tweets_with_hashtags(filename,country_hashtags,language_hashtags,user_hashtags,include_rt=False):
    with gzip.open(filename,'r') as f:
        for line in f:
            tweet = load_tweet_obj(line)
            if include_rt or not tweet['text'].startswith('RT'):
                country = tweet['user']['country']
                language = tweet['lang']
                user_id = tweet['user']['id_str']
                hashtags = get_hashtags_from_tweet_obj(tweet)
                for hashtag in hashtags:
                    country_hashtags[country][hashtag] += 1
                    language_hashtags[language][hashtag] += 1
                    user_hashtags[country][user_id][hashtag] += 1           

def write_entries(out_dir,hashtag_dict):
    for key in hashtag_dict:
        out_file = os.path.join(out_dir,key + '.json')
        with open(out_file,'w') as f:
            json.dump(hashtag_dict[key],f)


def main():

    tweet_dir = "/shared/2/projects/cross-lingual-exchange/data/relevant_tweets/all"
    out_dir = "/shared/2/projects/cross-lingual-exchange/data/temporal_hashtags_no_rt/"

    files_by_interval =  separate_files_by_interval(tweet_dir,14,range(2018,2021),datetime(2018,2,21))
    
    for interval in files_by_interval.keys():
        interval_dir = os.path.join(out_dir,str(interval))
        country_out_dir = os.path.join(interval_dir,'country')
        language_out_dir = os.path.join(interval_dir,'language')
        user_out_dir = os.path.join(interval_dir,'user')
        for out_path in [out_dir,interval_dir,country_out_dir,language_out_dir,user_out_dir]:
            if not os.path.exists(out_path):
                os.mkdir(out_path)
        country_hashtags = defaultdict(lambda: defaultdict(int))
        language_hashtags = defaultdict(lambda: defaultdict(int))
        user_hashtags =  defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
        for filename in files_by_interval[interval]:
            print(interval,filename)
            get_tweets_with_hashtags(filename,country_hashtags,language_hashtags,user_hashtags)

        print("writing country entries")
        write_entries(country_out_dir,country_hashtags)

        print("writing language entries")
        write_entries(language_out_dir,language_hashtags)

        print("writing user entries")
        write_entries(user_out_dir,user_hashtags)
        
    



    


if __name__ == "__main__":
    main()