from collections import defaultdict,Counter
import os
import ujson as json
import glob
import re
"""
Basic Idea
Init hashtag dict
d[hashtag] = Counter
d[hashtag][Topic 0] = 5 
For line i in multilingual tweets:
Get line i in topic list (this is the topic for the tweet)
For each word in line i tweet:
    if word is in the top hashtag set:
        d[word][topic] += 1

"""

base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
topic_model_data_dir = os.path.join(base_dir,'topic_model_data')
multilingual_text_dir = os.path.join(topic_model_data_dir,'multilingual_with_top100_hashtags') 
multilingual_topic_dir = os.path.join(topic_model_data_dir,f'multilingual_with_top100_hashtags_50topics_1Mtrain')
top_temporal_hashtag_file = os.path.join(topic_model_data_dir,'all_top100_hashtags.txt')
hashtags_by_topic_file = os.path.join(topic_model_data_dir,f'top100_hashtags_by_topic_50topics_1M.json')
top_hashtags_list = []
hashtag_topics = defaultdict(lambda: Counter())
with open(top_temporal_hashtag_file,'r') as f:
    for i,line in enumerate(f):
        top_hashtags_list.append(line.strip())
        if i % 10000 == 0:
            print(i)
print(top_hashtags_list[:5])
top_hashtags = set(top_hashtags_list)


multilingual_text_files = sorted(glob.glob(os.path.join(multilingual_text_dir,'*')))
multilingual_topic_files = sorted(glob.glob(os.path.join(multilingual_topic_dir,'*')))

for i,multilingual_text_file in enumerate(multilingual_text_files):
    print(i)
    multilingual_topic_file = multilingual_topic_files[i]
    tweet_topics = []
    with open(multilingual_topic_file,'r') as f:
        for line in f:
            tweet_topics.append(line.strip())
    with open(multilingual_text_file,'r') as f:
        for ix,line in enumerate(f):
            topic = tweet_topics[ix]
            words = [w.lower() for w in line.strip().split()]
            for word in words:
                if word[0] == '#':
                    hashtag = re.sub(r"^\W+|\W+$", "", word)
                    if hashtag in top_hashtags:
                        hashtag_topics[hashtag][topic] += 1



# assigned topic for each word is the one that has the highest value
# assigned_topic dict d[topic] = [words]
# [words] sorted by frequency  

assigned_topics = defaultdict(list)
for word in hashtag_topics:
    topic = hashtag_topics[word].most_common()[0][0]
    freq = hashtag_topics[word][topic]
    assigned_topics[topic].append((freq,word))

for topic in assigned_topics:
    assigned_topics[topic] = [w for w in sorted(assigned_topics[topic],reverse=True)]
    print(topic,assigned_topics[topic][:10])



with open(hashtags_by_topic_file,'w') as f:
    json.dump(assigned_topics,f)

