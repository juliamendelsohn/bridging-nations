import ujson as json
import csv
import pandas as pd
import os
import gzip

loc_file = '/shared/0/projects/location-inference/data/locations.2012-2019.from-merged.all.4-iter.tsv_iter_3.with-names.tsv.gz'
euro_country_code_file = '/shared/2/projects/cross-lingual-exchange/data/european_country_codes.tsv'
euro_country_codes = set(pd.read_csv(euro_country_code_file,sep='\t')['alpha-2'])
out_file = '/shared/2/projects/cross-lingual-exchange/data/european_users.json'


d = {}
with gzip.open(loc_file,'rt') as f:
    reader = csv.reader(f,delimiter='\t')
    for i,line in enumerate(reader):
        uid = line[0]
        city = line[3]
        country = line[5]
        if country in euro_country_codes:
            d[uid] = (city,country)
        if i % 100000 == 0:
            print(i)

with open(out_file,'w') as f:
    json.dump(d,f)
