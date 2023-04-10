import pandas as pd
import json
import os
from collections import Counter

# european_user_file = '/shared/2/projects/cross-lingual-exchange/data/european_users.json'
# with open(european_user_file,'r') as f:
#     user_dict = json.load(f)


old_dir = '/shared/0/projects/cross-lingual-exchange/data/dataframes/'
new_dir = '/shared/2/projects/cross-lingual-exchange/data/dataframes/'
# if not os.path.exists(new_dir):
#     os.mkdir(new_dir)

# filenames = ['de_pl.tsv','de_tr.tsv','es_pt.tsv']
# for filename in filenames:
#     df = pd.read_csv(os.path.join(old_dir,filename),sep='\t',dtype=str)
#     df.columns = ['user_id','lang','bilinguality','country','betw','num_tweets','followers',
#     'following','hashtag_count','url_count','degree']
#     cities = []
#     for uid in df['user_id']:
#         if uid in user_dict:
#             cities.append(user_dict[uid][0])
#         else:
#             cities.append('Not Found')
#     df['city'] = cities
#     print(df)
#     df.to_csv(os.path.join(new_dir,filename),sep='\t',index=False)

filenames = ['de_pl.tsv','de_tr.tsv','es_pt.tsv']
for filename in filenames:
    df = pd.read_csv(os.path.join(new_dir,filename),sep='\t',dtype=str)
    top_cities = []
    for country in set(df['country']):
        city_counts = Counter(df[df['country']==country]['city']).most_common(20)
        top_cities += [x[0] for x in city_counts]

    top_cities = set(top_cities)
    df['major_city'] = [x if x in top_cities else 'Not Found' for x in df['city']]
    df.to_csv(os.path.join(new_dir,filename),sep='\t',index=False)


