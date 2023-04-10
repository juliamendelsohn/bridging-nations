import json
import gzip
import glob
import os
from collections import defaultdict,Counter
import pandas as pd

country_codes_file = '/shared/2/projects/cross-lingual-exchange/data/european_country_codes.tsv'
df = pd.read_csv(country_codes_file,sep='\t').set_index('alpha-2')
code_to_name = df.to_dict()['name']


user_metadata_dir = '/shared/2/projects/cross-lingual-exchange/data/user_metadata_no_rt'
out_file = '/shared/2/projects/cross-lingual-exchange/data/top_langs_by_country_no_rt.json'
files = glob.glob(os.path.join(user_metadata_dir,'*.gz'))
country_langs = defaultdict(lambda: Counter() )
for filename in files:
    with gzip.open(filename,'r') as f:
        for i,line in enumerate(f):
            user_data = json.loads(line.decode('utf-8').strip())
            for lang in user_data['lang']:
                country_langs[user_data['country']][lang] += user_data['lang'][lang]


new_dict = {}
for country in country_langs:
    langs = country_langs[country].most_common()
    total_langs = sum(country_langs[country].values())
    country_name = code_to_name[country]
    new_dict[country_name] = [(x[0],x[1]/total_langs) for x in langs]
    new_dict[country_name] = [x for x in new_dict[country_name] if x[1] >= 0.01]

with open(out_file,'w') as f:
    json.dump(new_dict,f)