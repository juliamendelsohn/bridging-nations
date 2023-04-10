import csv
import os
import pandas as pd 
import ast
import random
from multiprocessing import Pool
import glob
import gzip
import ujson as json
from collections import defaultdict

data_dir = '/shared/2/projects/cross-lingual-exchange/data/'
tweet_dir = os.path.join(data_dir,'relevant_tweets')
df_country_to_code = pd.read_csv(os.path.join(data_dir,'european_country_codes.tsv'),sep='\t')
df_country_to_lang = pd.read_csv(os.path.join(data_dir,'country_languages.tsv'),sep='\t')
df_code_to_lang = df_country_to_code.merge(df_country_to_lang,left_on='alpha-2',right_on='country')[['country','language']].set_index('country')
code_to_lang = df_code_to_lang.to_dict()['language']
countries = list(df_code_to_lang.reset_index()['country'])
out_dir = os.path.join(data_dir,'sample_for_langid_eval')
if not os.path.exists(out_dir):
    os.mkdir(out_dir)


def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())

def get_text(tweet_obj):
    text = tweet_obj['text']
    if 'extended_tweet' in tweet_obj:
        text = tweet_obj['extended_tweet']['full_text']
    return text.replace('\n',' ').replace('\t',' ').replace('\r',' ')

def get_tweet_info_from_file(filename,lang):
    tweets = []
    with gzip.open(filename,'r') as f:
        for line in f:
            tweet_obj = load_tweet_obj(line)
            if tweet_obj['lang'] == lang:
                id_str = tweet_obj['id_str']
                text = get_text(tweet_obj)
                tweets.append((id_str,text,tweet_obj['lang']))
    return tweets


def sample_tweets(country,n=1000):
    print(country)
    lang = code_to_lang[country]
    country_tweet_dir = os.path.join(tweet_dir,country)
    out_file = os.path.join(out_dir,country + '.tsv')
    country_tweets = []

    country_tweet_files = glob.glob(os.path.join(country_tweet_dir,'*.gz'))
    for country_tweet_file in country_tweet_files:
        new_tweets = get_tweet_info_from_file(country_tweet_file,lang)
        country_tweets += new_tweets

    sample = random.sample(country_tweets,min(n,len(country_tweets)))
    df = pd.DataFrame(sample,columns=['id_str','text','twitter_lang']) 
    df.to_csv(out_file,sep='\t')



def main():
    pool = Pool(12)
    pool.map(sample_tweets,countries)

if __name__ =="__main__":
    main()
                    
