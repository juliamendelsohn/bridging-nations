import os
import json
import pandas as pd 
from itertools import combinations

# Criteria
# Countries must have different majority languages (L1 != L2)
# There must be at least X users from C1 and C2 
# There must be at least X monolinguals of L1 and L2
# There must be at least X bilinguals in L1-L2
# There must be at least X users from C1 with a neighbor in C2 and vice versa 


network_dir = '/shared/2/projects/cross-lingual-exchange/data/network_subsets/'
user_lang_dir = '/shared/2/projects/cross-lingual-exchange/data/user_language_info_no_rt'
neighbor_country_dir = '/shared/2/projects/cross-lingual-exchange/data/network_subset_country_info_no_rt/'
neighbor_language_dir = '/shared/2/projects/cross-lingual-exchange/data/network_subset_language_info_no_rt/'
country_lang_file = '/shared/2/projects/cross-lingual-exchange/data/country_languages.tsv'
country_to_language = pd.read_csv(country_lang_file,sep='\t').set_index('country').to_dict()['language']
out_file = '/shared/2/projects/cross-lingual-exchange/data/network_stats_for_filtering.tsv'


countries = list(country_to_language.keys())
country_pairs = [sorted(c) for c in combinations(countries,2)]
all_info = []

for (c1,c2) in country_pairs:
    l1 = country_to_language[c1]
    l2 = country_to_language[c2]
    if l1 == l2:
        print(f'Country pair {c1},{c2} have same language!')
    else:
        info = {}
        neighbor_language_file = os.path.join(neighbor_language_dir,f'{c1}_{c2}.gz')
        df = pd.read_json(neighbor_language_file,lines=True,compression='gzip')
        num_monolingual_c1 = df[df['country']==c1]['monolingual_majority'].sum()
        num_monolingual_c2 = df[df['country']==c2]['monolingual_majority'].sum()
        num_bilingual = df['is_bilingual'].sum()
        num_neighbors_other_country = df['has_neighbors_other_country'].sum()
        bilingual_neighbor = df['has_bilingual_neighbor'].sum()
        monolingual_neighbor = df['has_monolingual_neighbor'].sum()

        print(f'Country Pair: {c1}, {c2}')
        info['country pair'] = (c1,c2)
        info['monolingual c1'] = num_monolingual_c1
        info['monolingual c2'] = num_monolingual_c2
        info['bilingual'] = num_bilingual
        info['bilingual neighbor'] = bilingual_neighbor
        info['monolingual neighbor'] = monolingual_neighbor
        all_info.append(info)
    
df = pd.DataFrame(all_info)
df.to_csv(out_file,sep='\t')

        


