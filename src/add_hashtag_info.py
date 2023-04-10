import glob
import gzip
import bz2
import os
import csv
import ujson as json
import pandas as pd
from collections import defaultdict

user_hashtags = defaultdict(list)
for country_code in ['DE','PL','TR','ES','PT']:
    user_hashtag_file = f'/shared/2/projects/cross-lingual-exchange/data/hashtags/hashtags_by_country/{country_code}_users.json'
    with open(user_hashtag_file,'r') as f:
        d = json.load(f)
        for key in d:
            user_hashtags[key] = d[key]




def get_top_hashtags(filename,c1,c2):
    hashtags = defaultdict(list)
    df = pd.read_csv(filename,sep='\t')
    df.columns = ['word','logodds']
    hashtags[c1] = list(df[-5000:]['word'])
    hashtags[c2] = list(df[:5000]['word'])
    print(hashtags)
    return hashtags

def add_all_hashtag_info(old_df,country_hashtags,user_hashtags,c1,c2):
    user_ids = list(old_df['user_id'])
    all_info = []
    for uid in user_ids:
        if uid in user_hashtags:
            info = {}
            info['user_id'] = uid
            c1_hashtags = set(user_hashtags[uid]).intersection(set(country_hashtags[c1]))
            c2_hashtags = set(user_hashtags[uid]).intersection(set(country_hashtags[c2]))

            info[f'{c1}_num_hashtags'] = len(c1_hashtags)
            info[f'{c2}_num_hashtags'] = len(c2_hashtags)

            info[f'{c1}_frac_hashtags'] = len(c1_hashtags) / len(user_hashtags[uid])
            info[f'{c2}_frac_hashtags'] = len(c2_hashtags) / len(user_hashtags[uid])

            info[f'{c1}_jacc_hashtags'] = len(c1_hashtags) / len(set(user_hashtags[uid]).union(set(country_hashtags[c1])))
            info[f'{c2}_jacc_hashtags'] = len(c2_hashtags) / len(set(user_hashtags[uid]).union(set(country_hashtags[c2])))

            all_info.append(info)
    hash_df = pd.DataFrame(all_info)
    return hash_df

def main():
    logodds_path = '/shared/2/projects/cross-lingual-exchange/data/hashtags/logodds_country_hashtags/'
    datasheets_dir = '/shared/2/projects/cross-lingual-exchange/data/dataframes/'

    country_pairs = [('Germany','Poland'),('Germany','Turkey'),('Spain','Portugal')]
    country_code_pairs = [('DE','PL'),('DE','TR'),('ES','PT')]
    lang_pairs = [('de','pl'),('de','tr'),('es','pt')]
    
    for i in range(len(country_pairs)):
        c1 = country_pairs[i][0]
        c2 = country_pairs[i][1]
        l1 = lang_pairs[i][0]
        l2 = lang_pairs[i][1]
        a1 = country_code_pairs[i][0]
        a2 = country_code_pairs[i][1]
        logodds_file = os.path.join(logodds_path,f'hashtag_log_odds_{a1}_{a2}_scaled_unique_users.tsv')
        country_hashtags = get_top_hashtags(logodds_file,c1,c2)
        old_df = pd.read_csv(os.path.join(datasheets_dir,f'{l1}_{l2}_with_neighbor_info.tsv'),sep='\t',dtype=str)
        hash_df = add_all_hashtag_info(old_df,country_hashtags,user_hashtags,c1,c2)
        new_df = old_df.merge(hash_df,on='user_id')
        new_df.to_csv(os.path.join(datasheets_dir,f'{l1}_{l2}_with_hashtag_and_neighbor_info.tsv'),sep='\t',index=False)





if __name__ == "__main__":
    main()