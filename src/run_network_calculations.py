import os
import json
import csv
import gzip
import sys
import numpy as np
import graph_tool.all as gt
import argparse
from multiprocessing import Pool


network_subset_dir = "/shared/2/projects/cross-lingual-exchange/data/network_subsets"
network_calculations_dir = "/shared/2/projects/cross-lingual-exchange/data/network_calculations"
if not os.path.exists(network_calculations_dir):
    os.mkdir(network_calculations_dir)


def load_network(network_subset_dir,filename):
    g = gt.Graph(directed=False)
    network_subset_file = os.path.join(network_subset_dir,filename)
    edge_list = []
    with open(network_subset_file,'r') as f:
        for line in f:
            [uid1,uid2] = [u.strip() for u in line.split()]
            edge_list.append((uid1,uid2))
    index_to_uid = g.add_edge_list(edge_list, hashed=True)
    return g,index_to_uid




def calc_centralities(out_filename,g,index_to_uid,num_pivots = 5000):
    pr = gt.pagerank(g) # calc pagerank
    deg =  g.get_out_degrees(g.get_vertices())
    if num_pivots is not None:
        pivots = np.random.choice(g.get_vertices(), size=min(num_pivots,g.num_vertices(ignore_filter=True)), replace=False)
        betw,_ = gt.betweenness(g,pivots=pivots)
    else:
        betw,_= gt.betweenness(g)
    out_file = os.path.join(network_calculations_dir,out_filename)
    with open(out_file,'w') as f:
        fieldnames = ['uid','degree','betw','pagerank']
        writer = csv.DictWriter(f,fieldnames=fieldnames,delimiter='\t')
        writer.writeheader()
        for v in g.vertex_index:
            entry = {'uid':index_to_uid[v],'degree':deg[v], 'betw': betw[v], 'pagerank':pr[v]}
            writer.writerow(entry)   

def get_network_calculations(filename):
    print(f'{filename} starting')

    if filename in os.listdir(network_calculations_dir):
        print(f'{filename} already calculated!')
    else:
        g,index_to_uid = load_network(network_subset_dir,filename)
        num_vertices = g.num_vertices(ignore_filter=True)
        num_edges= g.num_edges(ignore_filter=True)
        print(f'{filename} Vertices: {num_vertices}, Edges: {num_edges}')
        calc_centralities(filename,g,index_to_uid)


def main():
    all_network_subset_files = sorted(os.listdir(network_subset_dir))
    pool = Pool(1)
    pool.map(get_network_calculations,all_network_subset_files)



if __name__ == "__main__":
    main()