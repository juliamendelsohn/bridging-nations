import os
import sys
import ujson
import glob
import spacy
from tqdm import tqdm
import operator


tweet_directory = sys.argv[1]
entity_directory = sys.argv[2]
l1 = sys.argv[3]
l2 = sys.argv[4]

os.chdir(tweet_directory)

l1_entities = {}
l2_entities = {}

for file in tqdm.tqdm(glob.glob('*.json'))
    with open(file, 'r') as f:
        print(file)

        all_tweets = ujson.load(f)

        for user, tweets in all_tweets.items():
            for tweet in tweets:
                if tweet['lang'] == l1:
                    for ent in tweet['entities'].keys():
                        if ent not in l1_entities:
                            l1_entities[ent] = {'count': 1, 'label': ent['label']}
                        else:
                            l1_entities[ent]['count'] += 1

                else:
                   for ent in tweet['entities'].keys():
                        if ent not in l2_entities:
                            l2_entities[ent] = {'count': 1, 'label': ent['label']}
                        else:
                            l2_entities[ent]['count'] += 1
                    

        with open(entity_directory + l1 + '_entities.json', 'w+') as outf:
            outf.write(ujson.dumps(l1_entities, indent=4))
        with open(entity_directory + l2 + '_entities.json', 'w+') as outf:
            outf.write(ujson.dumps(l2_entities, indent=4))
