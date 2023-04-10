import json
import gzip
import glob
import os
from collections import defaultdict,Counter
import pandas as pd
import csv


def load_country_codes(country_codes_file):
    df = pd.read_csv(country_codes_file,sep='\t').set_index('alpha-2')
    code_to_name = df.to_dict()['name'] 
    return code_to_name

def load_country_languages(country_lang_file):
    lang_to_country = defaultdict(list)
    with open(country_lang_file,'r') as f:
        reader = csv.reader(f,delimiter='\t')
        next(reader)
        for row in reader:
            country = row[0]
            lang = row[1]
            lang_to_country[lang].append(country)

    all_countries = []
    for lang in lang_to_country:
        all_countries += lang_to_country[lang]
    all_countries = set(all_countries)

    return lang_to_country,all_countries


def get_user_language_info(user_metadata_dir,out_dir,lang_to_country,all_countries,min_tweets=5,min_fraction=.1,und_thresh=0.2):
    

    files = glob.glob(os.path.join(user_metadata_dir,'*.gz'))
    for filename in files:
        print(filename)
        out_file = os.path.join(out_dir,os.path.basename(filename))
        if os.path.basename(filename)[:2] in all_countries:
            with gzip.open(filename,'r') as f_in, gzip.open(out_file,'w') as f_out:
                for i,line in enumerate(f_in):
                    user_data = json.loads(line.decode('utf-8').strip())
                    lang_profile = get_lang_profile(lang_to_country,user_data,min_tweets,min_fraction,und_thresh)
                    if lang_profile != None: # either none or a dict 
                        lang_profile_str = convert_entry_object_to_string(lang_profile)
                        f_out.write(lang_profile_str)
                                


def convert_entry_object_to_string(entry_obj):
	obj_str = json.dumps(entry_obj) + '\n'
	obj_bytes = obj_str.encode('utf-8')
	return obj_bytes 


def get_lang_profile(lang_to_country,user_data,min_tweets,min_fraction,und_thresh):
    lang_profile = {}
    lang_profile['id_str'] = user_data['id_str']
    lang_profile['country'] = user_data['country']

    if user_data['decahose_tweet_count'] < min_tweets:
        return None
    if 'und' in user_data['lang_norm'] and user_data['lang_norm']['und'] > und_thresh:
        return None
    langs = [x for x in user_data['lang_norm'] if user_data['lang_norm'][x] >= min_fraction and x != 'und']
    lang_profile['languages'] = langs
    
    if (len(langs) == 0) or (len(langs) > 2):
        return None
    if len(langs) == 1:
        if langs[0] not in lang_to_country:
            return None
        else:
            lang_profile['is_bilingual'] = 0
            lang_profile['monolingual_majority'] = 0
            lang_profile['monolingual_minority'] =0

            if user_data['country'] in lang_to_country[langs[0]]:
                lang_profile['monolingual_majority'] = 1
            else:
                lang_profile['monolingual_minority'] = 1
    if len(langs) == 2:
        if (langs[0] not in lang_to_country) or (langs[1] not in lang_to_country):
            return None
        else:
            lang_profile['is_bilingual'] = 1
            lang_profile['monolingual_majority'] = 0
            lang_profile['monolingual_minority'] = 0
    return lang_profile

        



def main():
    country_codes_file = '/shared/2/projects/cross-lingual-exchange/data/european_country_codes.tsv'
    country_lang_file = '/shared/2/projects/cross-lingual-exchange/data/country_languages.tsv'
    code_to_name = load_country_codes(country_codes_file)
    lang_to_country, all_countries = load_country_languages(country_lang_file)

    user_metadata_dir = '/shared/2/projects/cross-lingual-exchange/data/user_metadata_no_rt'
    out_dir = '/shared/2/projects/cross-lingual-exchange/data/user_language_info_no_rt'

    if not os.path.exists(out_dir):
        os.mkdir(out_dir)


    get_user_language_info(user_metadata_dir,out_dir,lang_to_country,all_countries)


    




if __name__ == "__main__":
    main()