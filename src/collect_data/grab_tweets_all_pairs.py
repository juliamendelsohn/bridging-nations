import glob
import gzip
import bz2
import os
import csv
import re
import ujson as json
from collections import defaultdict
from multiprocessing import Pool


print('getting global vars')

all_tweet_files = glob.glob('/twitter-turbo/decahose/raw/*.bz2')
european_user_file = '/shared/2/projects/cross-lingual-exchange/data/european_users.json'
out_dir = '/shared/2/projects/cross-lingual-exchange/data/relevant_tweets'
with open(european_user_file,'r') as f:
    user_dict = json.load(f)
essential_tweet_attributes = ['created_at','id_str','text','in_reply_to_status_id_str','in_reply_to_user_id_str',
'is_quote_status','entities','extended_tweet','lang']
essential_user_attributes = ['id_str','location','protected','verified','followers_count','friends_count','favourites_count',
'statuses_count','created_at','lang','profile_image_url','profile_image_url_https']
print('got user dict')

def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())

def convert_entry_object_to_string(entry_obj):
	obj_str = json.dumps(entry_obj) + '\n'
	obj_bytes = obj_str.encode('utf-8')
	return obj_bytes 

def write_entry(f_out,country,line): 
    tweet = load_tweet_obj(line)
    tweet_small = {}
    for attr in essential_tweet_attributes:
        if attr in tweet:
            tweet_small[attr] = tweet[attr]
    tweet_small['user'] = {}
    tweet_small['user']['country'] = country
    for attr in essential_user_attributes:
        if attr in tweet['user']:
            tweet_small['user'][attr] = tweet['user'][attr]

    entry_string = convert_entry_object_to_string(tweet_small)
    f_out.write(entry_string)



def file_complete(in_file):
    file_base_name = os.path.splitext(os.path.basename(in_file))[0]
    gb_file = os.path.join(out_dir,'GB',file_base_name + '.gz')
    return os.path.exists(gb_file)


def get_euro_tweets(in_file):
    try:
        with bz2.open(in_file,'r') as f_in:
            reg = re.compile("\"user\":.*?\"id_str\":\"([0-9]*)\"")
            file_base_name = os.path.splitext(os.path.basename(in_file))[0]
            out_file = os.path.join(out_dir,'all',file_base_name + '.gz')
            with gzip.open(out_file,'w') as f_out:
                for i,line in enumerate(f_in):
                    try:
                        reg_search = reg.search(line.decode('utf-8'))
                        if reg_search != None:
                            uid = reg_search.group(1)
                            if uid in user_dict:
                                country = user_dict[uid][1]
                                write_entry(f_out,country,line)
                
                        if i % 10000 == 0:
                            print(os.path.basename(in_file),i)
    
                    except:
                        continue          
    except:
        print(f"Error in processing file: {in_file}")


def main():
    pool = Pool(12)
    pool.map(get_euro_tweets,all_tweet_files)
    

if __name__ == "__main__":
    main()
