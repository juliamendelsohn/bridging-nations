import glob
import gzip
import os
import csv
import ujson as json
import pandas as pd
from collections import defaultdict
from itertools import combinations
import ast
from multiprocessing import Pool


network_dir = '/shared/2/projects/cross-lingual-exchange/data/network_subsets/'
user_lang_dir = '/shared/2/projects/cross-lingual-exchange/data/user_language_info_no_rt'
out_dir = '/shared/2/projects/cross-lingual-exchange/data/network_subset_language_info_no_rt2/'
country_lang_file = '/shared/2/projects/cross-lingual-exchange/data/country_languages.tsv'
user_country_file = '/shared/2/projects/cross-lingual-exchange/data/european_users.json'
country_to_lang = pd.read_csv(country_lang_file,sep='\t').set_index('country').to_dict()['language']


with open(user_country_file,'r') as f:
   user_countries = json.load(f)

if not os.path.exists(out_dir):
    os.mkdir(out_dir)


countries = list(country_to_lang.keys())
country_pairs = [sorted(c) for c in combinations(countries,2)]
#country_pairs = [['DE','PL'],['ES','PT'],['DE','TR']]



def get_neighbors(network_dir,country_pair):
    c1 = country_pair[0]
    c2 = country_pair[1]
    network_file = os.path.join(network_dir,f'{c1}_{c2}.tsv')
    neighbor_dict = defaultdict(set)

    with open(network_file,'r') as f:
        for row in f:
            [uid1,uid2] = [u.strip() for u in row.strip().split()]
            neighbor_dict[uid1].add(uid2)
            neighbor_dict[uid2].add(uid1)
    return neighbor_dict

def load_language_info(user_lang_dir,country_pair):
    c1_file = os.path.join(user_lang_dir,country_pair[0]+ '.gz')
    df1 = pd.read_json(c1_file,lines=True,compression='gzip',dtype=str).set_index('id_str')
    c2_file = os.path.join(user_lang_dir,country_pair[1] + '.gz')
    df2 = pd.read_json(c2_file,lines=True,compression='gzip',dtype=str).set_index('id_str')
    df = pd.concat([df1,df2])
    return df


def get_user_info(uid,country_pair,user_languages):
    if uid not in user_languages.index:
        return None
    info = user_languages.loc[[uid]]
    langs = ast.literal_eval(info['languages'][0])
    l1 = country_to_lang[country_pair[0]]
    l2 = country_to_lang[country_pair[1]]
    print(l1,l2,langs)
    if all([l in [l1,l2] for l in langs]):
        user_info = {}
        user_info['id_str'] = uid
        user_info['country'] = info['country'][0]
        user_info['monolingual_majority'] = info['monolingual_majority'][0]
        user_info['monolingual_minority'] = info['monolingual_minority'][0]
        user_info['is_bilingual'] = info['is_bilingual'][0]
        return user_info
    return None


def get_all_user_and_neighbor_info(country_pair):
    print(country_pair)
    neighbor_dict = get_neighbors(network_dir,country_pair)
    user_languages = load_language_info(user_lang_dir,country_pair)
    
    out_file = os.path.join(out_dir,f'{country_pair[0]}_{country_pair[1]}.gz')
    with gzip.open(out_file,'w') as f_out:
        for uid in neighbor_dict:
            user_info = get_user_info(uid,country_pair,user_languages)
            if user_info is not None:
                new_entry = {}
                neighbor_list = neighbor_dict[uid]
                neighbor_info = get_neighbor_info(neighbor_list,user_info,country_pair,user_languages)
                for key in user_info:
                    new_entry[key] = user_info[key]
                for key in neighbor_info:
                    new_entry[key] = neighbor_info[key]
                new_entry_str = convert_entry_object_to_string(new_entry)
                f_out.write(new_entry_str)



def convert_entry_object_to_string(entry_obj):
	obj_str = json.dumps(entry_obj) + '\n'
	obj_bytes = obj_str.encode('utf-8')
	return obj_bytes 


def get_neighbor_info(neighbor_list,user_info,country_pair,user_languages):
    info = defaultdict(int)
    info['num_neighbors'] = len(neighbor_list)
    info['num_neighbors_same_country'] = 0
    info['num_neighbors_other_country'] = 0
    info['num_neighbors_bilingual'] = 0
    info['num_neighbors_monolingual'] = 0

    for n in neighbor_list:
        if n in user_countries:
            neighbor_country = user_countries[n][1] # 0 index is for city 
            if neighbor_country == user_info['country']:
                info['num_neighbors_same_country'] += 1
            elif neighbor_country in country_pair:
                info['num_neighbors_other_country'] += 1
        neighbor_lang_info = get_user_info(n,country_pair,user_languages)
        if neighbor_lang_info != None:
            info['num_neighbors_bilingual'] += int(neighbor_lang_info['is_bilingual'])
            is_monolingual = int(neighbor_lang_info['monolingual_majority']) + int(neighbor_lang_info['monolingual_minority'])
            info['num_neighbors_monolingual'] += is_monolingual

    info['has_neighbors_same_country'] = 1 if info['num_neighbors_same_country'] > 0 else 0
    info['has_neighbors_other_country'] = 1 if info['num_neighbors_other_country'] > 0 else 0
    info['has_bilingual_neighbor'] = 1 if info['num_neighbors_bilingual'] > 0 else 0
    info['has_monolingual_neighbor'] = 1 if info['num_neighbors_monolingual'] > 0 else 0
    return info




    

def main():
    pool = Pool(12)
    pool.map(get_all_user_and_neighbor_info,country_pairs)
        

if __name__ == '__main__':
    main()
