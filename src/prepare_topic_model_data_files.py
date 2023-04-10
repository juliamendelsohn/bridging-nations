import glob
import gzip
import os
import csv
import ujson as json
from collections import defaultdict,Counter
import pandas as pd
import re
from multiprocessing import Pool
from itertools import product,repeat


# all files are in 
base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
tweet_dir = os.path.join(base_dir,'relevant_tweets','all')
country_lang_file = os.path.join(base_dir,'country_languages.tsv')
language_set = sorted(list(set(pd.read_csv(country_lang_file,sep='\t')['language'])))
topic_model_data_dir = os.path.join(base_dir,'topic_model_data')
english_text_file = os.path.join(topic_model_data_dir,'english2.txt')
multilingual_text_file = os.path.join(topic_model_data_dir,'multilingual2.txt')
text_with_top_hashtags_file = os.path.join(topic_model_data_dir,'multilingual_with_top100_hashtags.txt')
temporal_dirs = sorted(glob.glob(os.path.join(base_dir,'temporal_hashtags_no_rt','*','language','log_odds')))
top_temporal_hashtag_file = os.path.join(topic_model_data_dir,'all_top100_hashtags.txt')
top_hashtags_list = []
with open(top_temporal_hashtag_file,'r') as f:
    for i,line in enumerate(f):
        top_hashtags_list.append(line.strip())
        if i % 10000 == 0:
            print(i)
print(top_hashtags_list[:5])
top_hashtags = set(top_hashtags_list)



#GET TOP HASHTAGS FOR ALL LANGUAGES
def get_all_top_hashtags(temporal_dirs,top_temporal_hashtag_file,num_hashtags):
    hashtag_set = set()
    for temporal_dir in temporal_dirs:
        for language in language_set:
            print(temporal_dir,language)
            filename = os.path.join(temporal_dir,language+'.tsv')
            try:
                df = pd.read_csv(filename,sep='\t',header=None)
                df.columns = ['name','z-score']
                top = list(df[df['z-score'] > 1.96]['name'])
                if len(top) > num_hashtags:
                    top = top[:num_hashtags]
                hashtag_set |= set(top)
            except:
                continue
    hashtag_list = list(hashtag_set)
    with open(top_temporal_hashtag_file,'w') as f:
        for hashtag in hashtag_list:
            f.write(hashtag + '\n')



def preprocess_tweet(text):
    t = re.sub(r'\s', ' ', text)
    t = re.sub(r'@[^\s]+', '<user>', t)
    t = re.sub(r'http[^\s]+', '<url>', t)
    if t.startswith('RT'):
        t = t[2:]
    return t + '\n'


def get_tweets(filename,english_text_file,multilingual_text_file):
    print(filename)
    with open(english_text_file,'a') as f_out_en, open(multilingual_text_file,'a') as f_out_multi:
        with gzip.open(filename,'r') as input_file:
            for i,line in enumerate(input_file):
                tweet = json.loads(line.decode('utf-8').strip())
                text = preprocess_tweet(tweet['text'])
                lang = tweet['lang']
                if lang == 'en':
                    #write tweet text as a line in new file
                    f_out_en.write(text)
                elif lang in language_set:
                    #write tweet text as a line in other new file
                    f_out_multi.write(text)

def get_hashtags_from_tweet_obj(tweet):
    hashtags = set([h['text'].lower() for h in tweet['entities']['hashtags']])
    hashtags_extended = set()
    if 'extended_tweet' in tweet:
        hashtags_extended = set([h['text'].lower() for h in tweet['extended_tweet']['entities']['hashtags']])
    hashtags = hashtags.union(hashtags_extended)
    return list(hashtags)



def get_tweets_with_top_hashtags(filename,outfile):
    print(filename)
    with open(outfile,'a') as f:
        with gzip.open(filename,'r') as input_file:
            for i,line in enumerate(input_file):
                tweet = json.loads(line.decode('utf-8').strip())
                if tweet['lang'] in language_set:
                    tweet_hashtags = get_hashtags_from_tweet_obj(tweet)
                    if len(tweet_hashtags) > 0:
                        if any(hashtag in top_hashtags for hashtag in tweet_hashtags):
                            text = preprocess_tweet(tweet['text'])
                            f.write(text)

def main():
    if not os.path.exists(topic_model_data_dir):
        os.mkdir(topic_model_data_dir)
    #get_all_top_hashtags(temporal_dirs,top_temporal_hashtag_file,100)
    tweet_files = sorted(glob.glob(os.path.join(tweet_dir,'*')))
    pool = Pool(12)
    #pool.starmap(get_tweets,zip(tweet_files, repeat(english_text_file), repeat(multilingual_text_file)))
    pool.starmap(get_tweets_with_top_hashtags,zip(tweet_files,repeat(text_with_top_hashtags_file)))


if __name__ == "__main__":
    main()

            

