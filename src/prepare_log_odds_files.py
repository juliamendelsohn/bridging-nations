import os
import sys
import glob
import json
from collections import Counter


"""
This script should do the following. 
1. Make full counts file for hashtag/domain by country/language
2. Function to restrict to one hashtag/domain per user 
3. Function that gets the "rest" full counts (full counts - country/lang counts) and dumps to temp file
"""


def make_full_count_file(count_path):
    all_files = glob.glob(os.path.join(count_path,'*.json'))
    full_count = Counter()
    for i,filename in enumerate(all_files):
        print(i,filename)
        with open(filename,'r') as f:
            d = json.load(f)
        full_count += Counter(d)
    out_path = os.path.join(count_path,'full_count')
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    out_file = os.path.join(out_path,'full_count.json')
    with open(out_file,'w') as f:
        json.dump(full_count,f)





def main(): 
    #for interval in os.listdir(data_path):
        #interval_data_path = os.path.join(data_path,interval)
        #print(interval_data_path)
    info_type = 'domains' # 'domains' or  temporal hashtags
    data_path = f"/shared/2/projects/cross-lingual-exchange/data/{info_type}_no_rt/"

    for comparison_unit in ['country','language']:
        count_path = os.path.join(data_path,comparison_unit)
        make_full_count_file(count_path)



if __name__ == "__main__":
    main()


# countries = ['DE','PL','TR','PT','ES']
# for country in countries:
# 	user_count_file = os.path.join(wc_path,f'{country}_users.json')
# 	country_counts = Counter()
# 	country_counts_unique_users = Counter()
# 	with open(user_count_file,'r') as f:
# 		d = json.load(f)
# 		for key in d:
# 			hashtags = d[key]
# 			unique_hashtags = list(set(d[key]))
# 			for hashtag in hashtags:
# 				country_counts[hashtag] += 1
# 			for hashtag in unique_hashtags:
# 				country_counts_unique_users[hashtag] += 1
# 	out_file_unique_users = os.path.join(wc_path,f'{country}_counts_unique_users.json')
# 	out_file = os.path.join(wc_path,f'{country}_counts.json')

# 	with open(out_file,'w') as f:
# 		json.dump(country_counts,f)
# 	with open(out_file_unique_users,'w') as f:
# 		json.dump(country_counts_unique_users,f)

# country_count_files = [os.path.join(wc_path,f'{c}_counts.json') for c in countries]
# country_count_unique_users_files = [os.path.join(wc_path,f'{c}_counts_unique_users.json') for c in countries]

# full_counts = Counter()
# full_counts_unique_users = Counter()
# for filename in country_count_files:
# 	with open(filename,'r') as f:
# 		print(filename)
# 		d = json.load(f)
# 		full_counts += d 
# with open(os.path.join(wc_path,'full_counts.json'),'w') as f:
# 	json.dump(full_counts,f)

# for filename in country_count_unique_users_files:
# 	with open(filename,'r') as f:
# 		d = json.load(f)
# 		full_counts_unique_users += d 
# with open(os.path.join(wc_path,'full_counts_unique_users.json'),'w') as f:
# 	json.dump(full_counts_unique_users,f)