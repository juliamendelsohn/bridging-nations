from collections import defaultdict,Counter
import os
from typing import Hashable
import ujson as json
import glob
import re
import pandas as pd
import random 
import csv
import krippendorff
from itertools import combinations
import numpy as np


def get_true_labels(filename,num_choices):
    df = pd.read_csv(filename,sep='\t')
    df = df.drop(columns=['Unnamed: 0'])
    true_labels = []
    for _,row in df.iterrows():
        for i in range(num_choices):
            if row[i] == row['true']:
                true_labels.append(i+1)
                continue
    return true_labels








def get_human_labels(filename):
    df = pd.read_csv(filename,sep='\t')
    df.replace(' ',np.nan,inplace=True)
    return list(df['Intruder Word'])


def calculate_agreement(true_labels,annotator_labels,annotator_names):
    between_annotators = krippendorff.alpha(annotator_labels,level_of_measurement='nominal')
    annotator_pairs = list(combinations(annotator_names,2))
    agreement = {}
    labels = [annotator_labels[x] for x in annotator_names]
    agreement['all_annotators'] =  krippendorff.alpha(labels,level_of_measurement='nominal')
    for annotator_pair in annotator_pairs:
        labels = [annotator_labels[x] for x in annotator_pair] 
        agreement[annotator_pair] = krippendorff.alpha(labels,level_of_measurement='nominal')
    for annotator in annotator_names:
        labels = [true_labels,annotator_labels[annotator]]
        agreement[('true',annotator)] = krippendorff.alpha(labels,level_of_measurement='nominal')
    return agreement



def calculate_accuracy(true_labels,annotator_labels):
    annotator_accuracy = {}
    for annotator in annotator_labels:
        num_correct = 0
        num_total = len(true_labels)
        for i in range(num_total):
            true_label = true_labels[i]
            annotator_label = annotator_labels[annotator][i]
            if true_label == annotator_label:
                num_correct += 1
        accuracy = num_correct / num_total
        annotator_accuracy[annotator] = accuracy
    return annotator_accuracy

def calculate_accuracy_majority(true_labels,annotator_labels,majority_threshold):
    majority_labels = []
    for i in range(len(true_labels)):
        annot_labels = [annotator_labels[annotator][i] for annotator in annotator_labels]
        most_frequent_label,num_annots = Counter(annot_labels).most_common(1)[0]
        if num_annots >= majority_threshold:
            majority_labels.append(most_frequent_label)
        else:
            majority_labels.append('No majority')
    num_correct = 0
    num_total = len([x for x in majority_labels if x != 'No majority'])
    for i in range(len(true_labels)):
        if true_labels[i] == majority_labels[i]:
            num_correct += 1
    return num_correct / num_total
        





def main():
    topic_eval_dir = '/shared/2/projects/cross-lingual-exchange/data/topic_model_data/eval/'
    true_dir = os.path.join(topic_eval_dir,'true')
    human_dir = os.path.join(topic_eval_dir,'human')
    language='en'
    num_choices=5
    true_labels = get_true_labels(os.path.join(true_dir,f'{language}.tsv'),num_choices)
    annotator_labels = {}
    annotators=['david','ceren','julia']
    for annotator in annotators:
        filename = os.path.join(human_dir,f'{language}_{annotator}.tsv')
        annotator_labels[annotator] = get_human_labels(filename)
    print(annotator_labels)

    between_annotators = krippendorff.alpha([annotator_labels[annotator] for annotator in annotators],level_of_measurement='nominal')
    print(between_annotators)

    #true_labels = [l for (i,l) in enumerate(true_labels) if type(annotator_labels[annotator][i])==str]
    #annotator_labels[annotator] = [int(l) for l in annotator_labels[annotator] if type(l)==str]
    #print(annotator_labels[annotator])
    #print(true_labels)
    #agreement = calculate_agreement(true_labels,annotator_labels,annotators)

    # accuracy = calculate_accuracy(true_labels,annotator_labels)
    # results = []
    # for annotator in accuracy:
    #     results.append(['Accuracy',annotator,accuracy[annotator]])
    # results.append(['Accuracy','Average',sum([accuracy[a] for a in accuracy])/len(accuracy)])
    

    #accuracy_majority2 = calculate_accuracy_majority(true_labels,annotator_labels,2)
    #accuracy_majority3 = calculate_accuracy_majority(true_labels,annotator_labels,3)
    #results.append(['Accuracy','Majority2',accuracy_majority2])
    #results.append(['Accuracy','Majority3',accuracy_majority3])

    # for annotator in agreement:
    #     results.append(['Krippendorff',annotator,agreement[annotator]])
    # results.append(['Krippendorff','Average',sum([agreement[a] for a in combinations(annotators,2)])/3])

    # df = pd.DataFrame(results)
    # df.columns = ['Metric','Annotators','Score']

    # out_dir= os.path.join(topic_eval_dir,'agreement')
    # if not os.path.exists(out_dir):
    #     os.mkdir(out_dir)
    # out_file = os.path.join(out_dir,language+'.tsv')
    #print(df)
    #df.to_csv(out_file,sep='\t')


if __name__ == main():
    main()
