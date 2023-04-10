import gzip
from collections import *
import os
import csv
import snap
import sys

uid_to_country = {}
infile1 = sys.argv[1]
infile2 = sys.argv[2]

with open('/shared/0/projects/cross-lingual-exchange/data/user_lists/' + infile1 + '.tsv', 'r') as f:
    next(f)
    for line_no, line in enumerate(f, 1):
        cols = line.split('\t')
        
        if len(cols) == 3:
            uid = int(cols[1])
        
            if uid < 0:
                uid *= -1
            
            country = cols[2].strip()
            uid_to_country[uid] = country

        else:
            uid = int(cols[0])

            if uid < 0:
                uid *= -1

            country = cols[1].strip()
            uid_to_country[uid] = country

with open('/shared/0/projects/cross-lingual-exchange/data/user_lists/' + infile2 + '.tsv', 'r') as f:
    next(f)
    for line_no, line in enumerate(f, 1):
        cols = line.split('\t')

        uid = int(cols[1])

        if uid < 0:
            uid *= -1

        country = cols[2].strip()
        uid_to_country[uid] = country

with gzip.open('/shared/0/projects/location-inference/data/mention-network.2012-2020_04_09.tsv.gz', 'rb') as f:
    next(f)
    with open('/shared/0/projects/cross-lingual-exchange/data/user_lists/' + infile1 +'_' + infile2 + '.tsv', 'w+') as outf:
        for line in f:
            cols = line.decode().split('\t')
            uid1 = int(cols[0])
            uid2 = int(cols[1])
            
            if uid1 < 0:
                uid1 *= -1
            
            if uid2 < 0:
                uid2 *= -1
                
            if uid1 in uid_to_country and uid2 in uid_to_country:
                outf.write("{} {}\n".format(uid1, uid2))
