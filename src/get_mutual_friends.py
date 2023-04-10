import os
import pandas as pd
import json
from collections import defaultdict
from itertools import combinations,product
from multiprocessing import Pool
import csv 

#edge list file = 'de_tr.tsv' or whatever network subset
def get_neighbors(edge_list_file):
    neighbor_dict = defaultdict(set)
    with open(edge_list_file,'r') as f:
        for i,row in enumerate(f):
            [uid1,uid2] = [u.strip() for u in row.split(' ')]
            neighbor_dict[uid1].add(uid2)
            neighbor_dict[uid2].add(uid1)
    return neighbor_dict


def get_mutual_friends(neighbor_dict,out_file):
    friends_of_friends = defaultdict(set)
    num_users = len(neighbor_dict.keys())
    print(num_users)
    #loop through potential mutual friends
    for i,node in enumerate(neighbor_dict.keys()):
        pairs = combinations(neighbor_dict[node],2)
        for pair in pairs:
            (uid1,uid2) = sorted(pair) #to avoid duplicate keys
            # make sure uid1 and uid2 are not neighbors
            if (uid1 not in neighbor_dict[uid2]) and (uid2 not in neighbor_dict[uid1]):
                friends_of_friends[(uid1,uid2)].add(node)
        if i % 100 == 0:
            print(f'{i} out of {num_users} complete')
    
    with open(out_file, 'w') as csvfile:
        fieldnames = ['uid1', 'uid2','mutual_friends']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,delimiter='\t')
        writer.writeheader()
        for (uid1,uid2) in friends_of_friends.keys():
            writer.writerow({'uid1': uid1, 'uid2': uid2, 'mutual_friends':tuple(friends_of_friends[(uid1,uid2)])})
   
def run_script(country_pair):
    network_dir = '/shared/0/projects/cross-lingual-exchange/data/network_subsets'
    triad_dir = '/shared/2/projects/cross-lingual-exchange/data/triads'
    edge_list_file = os.path.join(network_dir,f"{country_pair[0]}_{country_pair[1]}.tsv")
    triads_file = os.path.join(triad_dir,f"{country_pair[0]}_{country_pair[1]}.tsv")
    neighbor_dict = get_neighbors(edge_list_file)
    print(country_pair,"got neighbors")
    get_mutual_friends(neighbor_dict,triads_file)

    

def main():
    
    country_pairs = [('DE','PL'),('DE','TR'),('ES','PT')]
    for country_pair in country_pairs:
        print(country_pair)
        with Pool(processes=3) as pool:
            pool.map(run_script,country_pairs)

if __name__ == '__main__':
    main()
