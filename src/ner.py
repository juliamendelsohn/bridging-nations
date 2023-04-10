import os
import sys
import ujson
import glob
import spacy
import operator

ner_lang = sys.argv[1]
ner_lib= sys.argv[2]

output_dir = sys.argv[3]
infile = sys.argv[4]

nlp_sys = spacy.load(ner_lib)


processed_files = glob.glob(
    output_dir+'*.json')

with open(infile, 'r') as f:
    if (output_dir + os.path.splitext(os.path.basename(infile))[0] + '_entities.json') not in processed_files:
        print(output_dir + os.path.splitext(os.path.basename(infile))[0] + '_entities.json')
        all_tweets = ujson.load(f)

        entities = {}

        for user, tweets in all_tweets.items():
            for tweet in tweets:
                if tweet['lang'] == ner_lang:
                    doc = nlp_sys(tweet['text'])
                    for ent in doc.ents:
                        if ent.text in entities:
                            entities[ent.text]['count'] += 1
                        else:
                            entities[ent.text] = {'count': 1, 'label': ent.label_}

        with open(output_dir + os.path.splitext(os.path.basename(infile))[0] + '_entitites.json', 'w+') as outf:
            outf.write(ujson.dumps(entities, indent=4))
