import glob
import gzip
import bz2
import os
import csv
import ujson as json
from itertools import product
from multiprocessing import Pool

all_tweet_files = glob.glob('/euler-twitter/storage*/gardenhose/raw/*/*/*.gz') + glob.glob('/twitter-turbo/decahose/raw/*.bz2')
european_user_file = '/shared/2/projects/cross-lingual-exchange/data/european_users.json'
out_dir = '/shared/2/projects/cross-lingual-exchange/data/hashtags/european_hashtags_by_tweet'
with open(european_user_file,'r') as f:
    user_dict = json.load(f)


def load_tweet_obj(line):
	return json.loads(line.decode('utf-8').strip())

def convert_entry_object_to_string(entry_obj):
	obj_str = json.dumps(entry_obj) + '\n'
	obj_bytes = obj_str.encode('utf-8')
	return obj_bytes 


def write_entries(outfile,entries):
	with gzip.open(outfile,'w') as f:
		for entry_obj in entries:
			entry_string = convert_entry_object_to_string(entry_obj)
			f.write(entry_string)

def get_hashtags_from_tweet_obj(tweet):
    hashtags = tweet['entities']['hashtags']
    hashtag_text = []
    for hashtag in hashtags:
        hashtag_text.append(hashtag['text'])
    return hashtag_text


def get_euro_tweets_with_hashtags(fp):
    all_entries = []
    for i,line in enumerate(fp):
        try:
            tweet = load_tweet_obj(line)
            hashtags = tweet['entities']['hashtags']
            if len(hashtags) > 0:
                uid = tweet['user']['id_str']
                if uid in user_dict:
                    new_entry = {}
                    new_entry['country'] = user_dict[uid][1]
                    new_entry['tweet_id'] = tweet['id_str']
                    new_entry['user_id'] = uid
                    new_entry['post_date'] = tweet['created_at']
                    new_entry['account_date'] = tweet['user']['created_at']
                    new_entry['hashtags'] = get_hashtags_from_tweet_obj(tweet)
                    all_entries.append(new_entry)
                    
        except:
            continue
    return all_entries


def read_file(tweet_file):
    print(tweet_file)
    try:
        out_file = os.path.join(out_dir,'.'.join(tweet_file.split('.')[1:-1]) + '.gz')
        file_extension = os.path.splitext(tweet_file)[1]
        if file_extension == '.bz2':
            with bz2.open(tweet_file,'r') as f:
                all_entries = get_euro_tweets_with_hashtags(f)
        elif file_extension == '.gz':
            with gzip.open(tweet_file,'r') as f:
                all_entries = get_euro_tweets_with_hashtags(f)
        write_entries(out_file,all_entries)
    except:
        print(f"Error in processing file: {tweet_file}")


def main():
    pool = Pool(12)
    pool.map(read_file,all_tweet_files)



if __name__ == "__main__":
    main()