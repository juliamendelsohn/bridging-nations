import pandas as pd
import csv 
import json
import os
import ast

country_lang = {}
country_lang['Germany'] = 'de'
country_lang['Turkey'] = 'tr'
country_lang['Spain'] = 'es'
country_lang['Portugal'] = 'pt'
country_lang['Poland'] = 'pl'

data_dir = '/shared/2/projects/cross-lingual-exchange/data/'
user_countries_file = os.path.join(data_dir,'user_countries.json')
user_langs_file = os.path.join(data_dir,'user_languages.json')

with open(user_countries_file) as f:
    user_countries = json.load(f)

with open(user_langs_file) as f2:
    user_langs = json.load(f2)


def filter_triads(all_triads_file):
    with open(all_triads_file, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            uid1 = row['uid1']
            uid2 = row['uid2']
            mutuals = ast.literal_eval(row['mutual_friends'])
            if (uid1 in user_countries) and (uid2 in user_countries):
                c1 = user_countries[uid1]
                c2 = user_countries[uid2]
                l1 = user_langs[uid1]
                l2 = user_langs[uid2]
                if (c1 != c2) and (l1 == country_lang[c1]) and (l2 == country_lang[c2]):
                    if l2 < l1: #switch order to make alphabetical
                        uid1 = row['uid2']
                        uid2 = row['uid1']
                    # get language status of mutual friends

def get_mutual_info(mutual_friends):
    info = {}
    


def main():
    country_pairs = [('DE','PL'),('DE','TR'),('ES','PT')]
    all_triads_dir = '/shared/2/projects/cross-lingual-exchange/data/triads/'
    all_triads_file = os.path.join(all_triads_dir,'DE_PL.tsv')

    filter_triads()

if __name__ == "__main__":
    main()