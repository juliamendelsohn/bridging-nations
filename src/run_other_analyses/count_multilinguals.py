import gzip
import pandas as pd 
import glob
import os

user_lang_info_dir = '/shared/2/projects/cross-lingual-exchange/data/user_language_info_no_rt'
# outfile = '/shared/2/projects/cross-lingual-exchange/data/country_user_counts.tsv'
included_countries = [x[:2] for x in os.listdir(user_lang_info_dir)]
# user_counts = []
# for country in countries:
#     print(country)
#     df = pd.read_json(os.path.join(user_lang_info_dir,country+'.gz'),lines=True)
#     df = df.drop_duplicates(subset=['id_str'])
#     user_count = {}
#     user_count['country'] = country
#     user_count['num_bilinguals'] = df['is_bilingual'].sum()
#     user_count['num_monolingual_majority'] = df['monolingual_majority'].sum()
#     user_count['num_monolingual_minority'] = df['monolingual_minority'].sum()
#     user_count['total'] = len(df)
#     user_count['bilingual_percent'] = user_count['num_bilinguals'] / user_count['total']
#     user_counts.append(user_count)

# df = pd.DataFrame(user_counts)
# df.to_csv(outfile,sep='\t')



all_users_metadata_dir = '/shared/2/projects/cross-lingual-exchange/data/user_metadata_no_rt'

user_metadata_countries = [x[:2] for x in os.listdir(all_users_metadata_dir)]



all_counts = []

for country in user_metadata_countries:
    print(country)
    res = {}
    fname = os.path.join(all_users_metadata_dir,country+'.gz')
    df = pd.read_json(fname,lines=True)
    df = df.drop_duplicates(subset=['id_str'])
    res['country'] = country
    res['is_included'] = (country in included_countries)
    res['total'] = len(df)
    res['pass_min_tweet'] = len(df[df['decahose_tweet_count'] >= 5])
    und_prop = []
    for x in df['lang_norm']:
        if 'und' in x:
            und_prop.append(x['und'])
        else:
            und_prop.append(0)
    df['und_prop'] = und_prop
    res['pass_und_thresh'] = len(df[df['und_prop'] <= 0.2])
    all_counts.append(res)

df = pd.DataFrame(all_counts)
print(df)
print(df.sum())
df.to_csv('/shared/2/projects/cross-lingual-exchange/data/full_user_counts.tsv',sep='\t')
