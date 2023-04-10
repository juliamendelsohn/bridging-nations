import glob
import gzip
import os
import ujson as json
from multiprocessing import Pool
from collections import defaultdict,Counter
from datetime import datetime,timedelta
import pandas as pd
from urllib.parse import urlparse



euro_country_code_file = '/shared/2/projects/cross-lingual-exchange/data/european_country_codes.tsv'
tweet_dir = "/shared/2/projects/cross-lingual-exchange/data/relevant_tweets/all"
out_dir = "/shared/2/projects/cross-lingual-exchange/data/user_metadata_no_rt/"
if not os.path.exists(out_dir):
    os.mkdir(out_dir)


def get_country_list(euro_country_code_file):
    return list(set(pd.read_csv(euro_country_code_file,sep='\t')['alpha-2']))


#Most recent files looked at first (to most easily get follower counts, etc.)
def order_files_chronologically(tweet_dir,year_range):
    all_tweet_files = []
    for year in year_range:
        for m in range(1,13):
            month = str(m)
            if len(month) == 1:
                month = '0' + month
            for d in range(1,32):
                day = str(d)
                if len(day) == 1:
                    day = '0' + day
                tweet_files_1 = glob.glob(os.path.join(tweet_dir,f'decahose.{year}-{month}-{day}*.gz'))
                tweet_files_2 = glob.glob(os.path.join(tweet_dir,f'decahose.{year}{month}{day}*.gz'))
                all_tweet_files += tweet_files_1
                all_tweet_files += tweet_files_2
    return all_tweet_files[::-1]

def read_file(tweet_file,country_list,user_info,include_rt=False):
    country_set = set(country_list)
    with gzip.open(tweet_file,'r') as f:
        for line in f:
            try:
                tweet = json.loads(line.decode('utf-8').strip())
                if include_rt or not tweet['text'].startswith('RT'):
                    uid = tweet['user']['id_str']
                    if tweet['user']['country'] in country_set:
                        if uid not in user_info:
                            user_info[uid] = get_new_user_info(tweet)
                        user_info[uid]['lang'][tweet['lang']] += 1
                        user_info[uid]['decahose_tweet_count'] += 1
                        hashtags = get_hashtags(tweet)
                        domains = get_domains(tweet)
                        for hashtag in hashtags:
                            user_info[uid]['hashtags'][hashtag] += 1
                        for domain in domains:
                            user_info[uid]['domains'][domain] += 1
            except:
                continue


def get_hashtags(tweet):
    hashtags = [h['text'].lower() for h in tweet['entities']['hashtags']]
    if 'extended_tweet' in tweet:
        hashtags_extended = [h['text'].lower() for h in tweet['extended_tweet']['entities']['hashtags']]
        if len(hashtags_extended) > 0:
            hashtags += hashtags_extended
    hashtags = list(set(hashtags))
    return hashtags


def get_domains(tweet):
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


def get_account_age(tweet):
    post_time_str = tweet['created_at']
    account_time_str = tweet['user']['created_at']
    post_time_obj = datetime.strptime(post_time_str,'%a %b %d %H:%M:%S +0000 %Y')
    account_time_obj = datetime.strptime(account_time_str,'%a %b %d %H:%M:%S +0000 %Y')
    account_age = post_time_obj - account_time_obj
    return account_age.days


def get_new_user_info(tweet):
    new_user_info = {}
    #user_info['id_str'] = tweet['user']['id_str']
    new_user_info['country'] = tweet['user']['country']
    new_user_info['verified'] = tweet['user']['verified']
    new_user_info['friends_count'] = tweet['user']['friends_count']
    new_user_info['followers_count'] = tweet['user']['followers_count']
    new_user_info['favourites_count'] = tweet['user']['favourites_count']
    new_user_info['statuses_count'] = tweet['user']['statuses_count']
    new_user_info['account_age'] = get_account_age(tweet)
    if new_user_info['account_age'] == 0:
        new_user_info['post_rate'] = 0
    else:
        new_user_info['post_rate'] = new_user_info['statuses_count'] / new_user_info['account_age']
    new_user_info['decahose_tweet_count'] = 0
    new_user_info['lang'] = Counter()
    new_user_info['hashtags'] = Counter()
    new_user_info['domains'] = Counter()
    return new_user_info

    
def write_user_info(country_list,user_info):
    for country in country_list:
        out_file = os.path.join(out_dir,country+'.gz')
        with gzip.open(out_file,'w') as f:
            for uid in user_info:
                entry = user_info[uid]
                if entry['country'] == country:
                    entry['id_str'] = uid
                    total_langs = sum(entry['lang'].values())
                    entry['lang_norm'] = {}
                    for lang in entry['lang']:
                        entry['lang_norm'][lang] = entry['lang'][lang] / total_langs
                    obj_str = json.dumps(entry) + '\n'
                    obj_bytes = obj_str.encode('utf-8')
                    f.write(obj_bytes)


def get_users_for_specific_countries(country_list):
    all_tweet_files = order_files_chronologically(tweet_dir,range(2018,2021))
    user_info = {}
    for i,tweet_file in enumerate(all_tweet_files):
        print(country_list[0],i)
        read_file(tweet_file,country_list,user_info)
    write_user_info(country_list,user_info)

def main():
    pool = Pool(5)
    countries = get_country_list(euro_country_code_file)
    big_countries = ['GB', 'ES', 'TR', 'FR', 'NL', 'RU']
    countries = [c for c in countries if c not in big_countries]
    country_lists = [countries,['ES'],['TR'],['FR','NL','RU'],['GB']]
    pool.map(get_users_for_specific_countries,country_lists)
    



if __name__ == "__main__":
    main()
