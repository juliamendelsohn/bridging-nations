import glob
import gzip
import bz2
import os
import csv
import ujson as json
from itertools import product
from multiprocessing import Pool
from collections import defaultdict,Counter
from urllib.parse import urlparse


def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())


def get_domains_from_tweet_obj(tweet):
    urls = [u['expanded_url'] for u in tweet['entities']['urls']]
    if 'extended_tweet' in tweet:
        urls_extended = [u['expanded_url'] for u in tweet['extended_tweet']['entities']['urls']]
        if len(urls_extended) > 0:
            urls += urls_extended
    urls = list(set(urls))
    domains = [convert_url_to_domain(u) for u in urls]
    return domains

def convert_url_to_domain(url):
    domain = urlparse(url).netloc.lower()
    domain =  domain.replace('www.','')
    if domain.startswith('feedproxy'):
        domain = urlparse(url).path.split('/')[2].lower()
    return domain


def get_tweets_with_domains(filename,country_domains,language_domains,include_rt=False):
    with gzip.open(filename,'r') as f:
        for i,line in enumerate(f):
            try:
                tweet = load_tweet_obj(line)
                if include_rt or not tweet['text'].startswith('RT'):
                    country = tweet['user']['country']
                    language = tweet['lang']
                    if country not in country_domains:
                        country_domains[country] = Counter()
                    if language not in language_domains:
                        language_domains[language] = Counter()
                    domains = get_domains_from_tweet_obj(tweet)
                    for domain in domains:
                        country_domains[country][domain] += 1
                        language_domains[language][domain] += 1
            except:
                continue


def write_entries(out_dir,domain_dict):
    for key in domain_dict:
        out_file = os.path.join(out_dir,key + '.json')
        with open(out_file,'w') as f:
            json.dump(domain_dict[key],f)
	

def main():

    tweet_dir = "/shared/2/projects/cross-lingual-exchange/data/relevant_tweets/all"
    out_dir = "/shared/2/projects/cross-lingual-exchange/data/domains_no_rt/"
    country_out_dir = os.path.join(out_dir,'country')
    language_out_dir = os.path.join(out_dir,'language')
    for out_path in [out_dir,country_out_dir,language_out_dir]:
        if not os.path.exists(out_path):
            os.mkdir(out_path)

    country_domains = {}
    language_domains = {}
    all_tweet_files = glob.glob(os.path.join(tweet_dir,'*.gz'))
    for i,filename in enumerate(all_tweet_files):
        print(i,filename)
        get_tweets_with_domains(filename,country_domains,language_domains)
    
    print("writing country entries")
    write_entries(country_out_dir,country_domains)

    print("writing language entries")
    write_entries(language_out_dir,language_domains)
    
    



    


if __name__ == "__main__":
    main()
