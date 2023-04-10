import gzip
from collections import *
import os
import csv
import snap
import sys
from itertools import combinations
import pandas as pd
import json



def get_all_euro_pairs(euro_country_code_file):
    euro_country_codes = list(set(pd.read_csv(euro_country_code_file,sep='\t')['alpha-2']))
    country_pairs = list(combinations(euro_country_codes,2))
    country_pairs = [sorted(x) for x in country_pairs]
    return country_pairs


def get_euro_network_subsets(network_file,country_pairs,european_users):
    subset_edges = {}
    for country_pair in country_pairs:
        subset_edges[tuple(country_pair)] = []
    with gzip.open(network_file,'rt') as f:
        reader = csv.DictReader(f,delimiter='\t')
        for i,row in enumerate(reader):
            uid1 = str(row['uid1'])
            uid2 = str(row['uid2'])
            if (uid1 in european_users) and (uid2 in european_users):
                country1 = european_users[uid1][1]
                country2 = european_users[uid2][1]
                if country1 != country2:
                    subset_edges[tuple(sorted([country1,country2]))].append((uid1,uid2))
                elif country1 == country2:
                    for country_pair in subset_edges.keys():
                        if country1 in country_pair:
                            subset_edges[country_pair].append((uid1,uid2))

    return subset_edges

def write_network_subsets(out_dir,subset_edges):
    for country_pair in subset_edges:
        filename = f'{country_pair[0]}_{country_pair[1]}.tsv'
        with open(os.path.join(out_dir,filename),'w') as f:
            writer = csv.writer(f,delimiter='\t')
            for user_pair in subset_edges[country_pair]:
                writer.writerow(user_pair)


def main():
    european_user_file = '/shared/2/projects/cross-lingual-exchange/data/european_users.json'
    with open(european_user_file,'r') as f:
        european_users = json.load(f)
    euro_country_code_file = '/shared/2/projects/cross-lingual-exchange/data/european_country_codes.tsv'
    network_file = '/shared/0/projects/location-inference/data/mention-network.2012-2020_04_09.tsv.gz'
    out_dir = '/shared/2/projects/cross-lingual-exchange/data/network_subsets/'
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    country_pairs = get_all_euro_pairs(euro_country_code_file)
    subset_edges = get_euro_network_subsets(network_file,country_pairs,european_users)
    write_network_subsets(out_dir,subset_edges)
   

if __name__ == "__main__":
    main() 