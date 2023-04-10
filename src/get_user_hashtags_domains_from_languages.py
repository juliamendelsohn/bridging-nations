import csv
import os
import pandas as pd 
import ast
from multiprocessing import Pool
import glob
import gzip
import ujson as json
from collections import defaultdict

base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
network_stats_file = os.path.join(base_dir,'network_stats_for_filtering.tsv')

country_lang_file = os.path.join(base_dir,'country_languages.tsv')
language_set = sorted(list(set(pd.read_csv(country_lang_file,sep='\t')['language'])))
country_set = sorted(list(set(pd.read_csv(country_lang_file,sep='\t')['country'])))


user_metadata_dir = os.path.join(base_dir,'user_metadata')
temporal_dirs_user = sorted(glob.glob(os.path.join(base_dir,'temporal_hashtags','*','user')))

# if we exclude retweets, add _no_rt to all directories
hashtag_dir = os.path.join(base_dir,'hashtags','language','log_odds')
domain_dir = os.path.join(base_dir,'domains','language','log_odds')
temporal_dirs = sorted(glob.glob(os.path.join(base_dir,'temporal_hashtags','*','language','log_odds')))
num_intervals = len(temporal_dirs)
out_dir = os.path.join(base_dir,'user_language_intersections')


hashtag_out_dir = os.path.join(out_dir,'hashtags_top100')
domain_out_dir = os.path.join(out_dir,'domains_top100')
temporal_out_dir = os.path.join(out_dir,'temporal_hashtags_top100')
for directory in [out_dir,hashtag_out_dir,domain_out_dir,temporal_out_dir]:
   if not os.path.exists(directory):
      os.mkdir(directory)

def get_top_by_language(info_dir,topn=2000):
   top_info = {}
   for language in language_set:
      filename = os.path.join(info_dir,language + '.tsv')
      try:
         df = pd.read_csv(filename,sep='\t',header=None)
         df.columns = ['name','z-score']
         top = list(df[df['z-score'] > 1.96]['name'])
         if len(top) > topn:
            top = top[:topn]
         top_info[language] = set(top)
      except:
         continue
   return top_info


top_hashtags_temporal = {}
for i in range(num_intervals):
   print(i)
   interval_dir = os.path.join(base_dir,'temporal_hashtags_no_rt',str(i),'language','log_odds')
   top_hashtags_temporal[i] = get_top_by_language(interval_dir,topn=100)

assigned_topics_file = os.path.join(base_dir,'topic_model_data','top100_hashtags_by_topic_50topics_1M.json')
hashtag_to_topic = {}
with open(assigned_topics_file,'r') as f:
   topic_to_hashtag = json.load(f)
   for topic in topic_to_hashtag:
      for hashtag_info in topic_to_hashtag[topic]:
         hashtag = hashtag_info[1]
         hashtag_to_topic[hashtag] = topic


top_hashtags = get_top_by_language(hashtag_dir,topn=100)
top_domains = get_top_by_language(domain_dir,topn=100)


def get_intersections(country):
   print(country)
   
   filename = os.path.join(user_metadata_dir,country + '.gz')
   hashtags_by_language = defaultdict(lambda: defaultdict(int))
   domains_by_language = defaultdict(lambda: defaultdict(int))
   with gzip.open(filename,'r') as f:
      for line in f:
         row = json.loads(line.decode('utf-8').strip())
         id_str = row['id_str']
         user_hashtags = set(row['hashtags'].keys())
         user_domains = set(row['domains'].keys())
      
         for language in language_set:            
            hashtag_intersect = top_hashtags[language].intersection(user_hashtags)
            domain_intersect = top_domains[language].intersection(user_domains)
            if len(hashtag_intersect) > 0:
               hashtags_by_language[id_str][language] = len(hashtag_intersect)
            if len(domain_intersect) > 0:
               domains_by_language[id_str][language] = len(domain_intersect)
    
   hashtag_filename = os.path.join(hashtag_out_dir,country + '.json')
   domain_filename = os.path.join(domain_out_dir,country + '.json')
   with open(hashtag_filename,'w') as f:
      json.dump(hashtags_by_language,f)
   with open(domain_filename,'w') as f:
      json.dump(domains_by_language,f)




def get_temporal_hashtag_intersections(country,topical_analysis=True):
   temporal_hashtags_by_language = defaultdict(lambda: defaultdict(int))
   temporal_hashtags_by_lang_topic = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

   for i in range(num_intervals):
      print(country,i)
      temporal_dirs_user = [os.path.join(base_dir,'temporal_hashtags',str(i),'user')]
      if i < num_intervals - 1:
         temporal_dirs_user.append(os.path.join(base_dir,'temporal_hashtags',str(i+1),'user'))
      
      user_hashtags = defaultdict(list)
      for directory in temporal_dirs_user:
         filename = os.path.join(directory,country + '.json')
         with open(filename,'r') as f:
            user_hash = json.load(f)
         for id_str in user_hash:
            new_hashtags = list(user_hash[id_str].keys())
            user_hashtags[id_str] = list(set(new_hashtags + user_hashtags[id_str]))
         
      for language in top_hashtags_temporal[i]:
         for id_str in user_hashtags:
            hashtag_intersect = top_hashtags_temporal[i][language].intersection(user_hashtags[id_str])
            if len(hashtag_intersect) > 0:
               temporal_hashtags_by_language[id_str][language] += len(hashtag_intersect)
               if topical_analysis:
                  for hashtag in list(hashtag_intersect):
                     if hashtag in hashtag_to_topic:
                        topic = hashtag_to_topic[hashtag]
                        temporal_hashtags_by_lang_topic[id_str][language][topic] += 1


   temporal_hashtag_filename = os.path.join(temporal_out_dir,country + '.json')
   with open(temporal_hashtag_filename,'w') as f:
      json.dump(temporal_hashtags_by_language,f)

   if topical_analysis:
      temporal_hashtag_topic_filename = os.path.join(temporal_out_dir,country + '_with_topic.json')
      with open(temporal_hashtag_topic_filename,'w') as f:
         json.dump(temporal_hashtags_by_lang_topic,f)
 


def main():
   pool = Pool(12)
   pool.map(get_intersections,country_set)
   pool.map(get_temporal_hashtag_intersections,country_set)
   


if __name__ == "__main__":
      main()


"""
For each country pair 
Get top hashtags of l1, l2 (country_to_lang)
Get top domains of l1, l2 (country_to_lang)
For each time interval, get top temporal hashtags for L1, L2 


For each user of country c1 in (C1,C2)
If user is monolingual in l1: 
 - Get set of user hashtags 
    - Calculate binary/size of intersection: user hashtag with Top l2 hashtags
 - Get set of user domains
    - Calculate binary/size of intersection: user domains with Top L2 domains
 - for each time interval, get set of temporal hashtags for user
    - For time interval i:
        - get binary/size of intersection: L2 hashtags from i, user hashtags from i AND i+1
    - Number of common hashtags as total sum
    - Binary uses L2 hashtag if intersection is not empty for at least one time interval
 

For each user of country c2 in (C1,C2)
If user is monolingual in l2: 
 - Get set of user hashtags 
 - Get set of user domains
 - for each time interval, get set of temporal hashtags for user
"""