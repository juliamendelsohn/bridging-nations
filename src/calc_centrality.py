import os
import json
import csv
import gzip
import sys
import numpy as np

import graph_tool.all as gt

import argparse
import pandas as pd

parser = argparse.ArgumentParser()
parser.add_argument("-a", "--approx", help="calculate approximate betweenness")
parser.add_argument("-n", "--network",
                    help="appropriate network susbet for graph", required=True)
parser.add_argument("-l1", "--l1", help="1st language", required=True)
parser.add_argument("-l2", "--l2", help="2nd language", required=True)
args = parser.parse_args()

network_subset = args.network


with open('/shared/0/projects/cross-lingual-exchange/data/temp/relevant_users_with_lang.json', 'r') as f:
    user_data = json.load(f)

relevant_users = {}
for user, user_profile in user_data.items():
    if user_profile['num_tweets'] > 1:
        relevant_users[user] = user_profile

size = 0

g = gt.Graph(directed=False)

edge_list = []

with open(network_subset) as f:
    for line in f:
        uid1 = line.split(' ')[0].strip()
        uid2 = line.split(' ')[1].strip()

        edge_list.append((uid1, uid2))

name = g.add_edge_list(edge_list, hashed=True)

# calculate page_rank
pr = gt.pagerank(g)

# calculate betweenness_centrality

if args.approx is not None:
    vertices = g.get_vertices()
    pivots = np.random.uniform(vertices.shape[0], size=np.int(
        float(args.approx) * vertices.shape[0]))
    vp_betw, _ = gt.betweenness(g, pivots=pivots)

else:
    vp_betw, _ = gt.betweenness(g)


for v in g.vertices():
    if name[v] in relevant_users:
        relevant_users[name[v]]['page_rank'] = pr[v]
        relevant_users[name[v]]['betw'] = vp_betw[v]
        relevant_users[name[v]]['degree'] = v.out_degree()


user_pr = {'user_id': [], 'lang': [], 'bilinguality': [], 'country': [], 'betw': [],
           'num_tweets': [], 'followers': [],
           'following': [], 'hashtag_count': [],
           'url_count': [], 'degree': []}
for user, user_data in relevant_users.items():
    if 'betw' in user_data:
        user_pr['user_id'].append(user)

        if user_data['bilinguality'] >= 0.99:
            user_pr['lang'].append(args.l1)

        elif user_data['bilinguality'] <= 0.01:
            user_pr['lang'].append(args.l2)
        else:
            user_pr['lang'].append('BI')

        user_pr['bilinguality'].append(user_data['bilinguality'])
        user_pr['country'].append(user_data['country'].rstrip())
        user_pr['betw'].append(user_data['betw'])
        user_pr['num_tweets'].append(user_data['num_tweets'])
        user_pr['followers'].append(user_data['followers'])
        user_pr['following'].append(user_data['following'])
        user_pr['hashtag_count'].append(user_data['hashtag_count'])
        user_pr['url_count'].append(user_data['url_count'])
        user_pr['degree'].append(user_data['degree'])


columns = user_pr.keys()
df = pd.DataFrame(user_pr, columns=columns)
df.to_csv('/shared/0/projects/cross-lingual-exchange/data/dataframes/' +
          args.l1 + '_' + args.l2 + '.tsv', sep='\t', header=False, index=False)
