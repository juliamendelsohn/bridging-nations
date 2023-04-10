from contextualized_topic_models.models.ctm import ZeroShotTM
from contextualized_topic_models.utils.data_preparation import TopicModelDataPreparation
from contextualized_topic_models.utils.preprocessing import WhiteSpacePreprocessing
import nltk
nltk.download('stopwords')
import glob
import numpy as np
import os
import pickle
import re
os.environ["TOKENIZERS_PARALLELISM"] = "false"



def load_data(text_file):
    documents = []
    with open(text_file,'r',encoding='utf-8') as f:
        for i,line in enumerate(f):
            text = line.replace('<user>','').replace('<url>','').strip()
            documents.append(text)
            if i % 1000000 == 0:
                print(i)
    return documents

def prepare_training_dataset(english_text_file):
    english_documents = load_data(english_text_file)
    sp = WhiteSpacePreprocessing(english_documents, stopwords_language='english')
    preprocessed_documents, unpreprocessed_corpus, vocab = sp.preprocess()
    print(preprocessed_documents[:3])

    tp = TopicModelDataPreparation("paraphrase-multilingual-mpnet-base-v2")
    training_dataset = tp.fit(text_for_contextual=unpreprocessed_corpus, text_for_bow=preprocessed_documents)
    return tp,training_dataset

def train_ctm(tp,training_dataset,num_topics,num_epochs,outdir):
    models_outdir = os.path.join(outdir,'ctm')
    tp_file = os.path.join(outdir,'tp.p')
    topics_file = os.path.join(outdir,'topic_keywords.txt')
    ctm = ZeroShotTM(bow_size=len(tp.vocab), contextual_size=768, n_components=num_topics, num_epochs=num_epochs)
    ctm.fit(training_dataset) # run the model
    ctm.save(models_dir=models_outdir)

    topic_keywords = ctm.get_topic_lists(10)
    with open(topics_file,'w') as f:
        for i,topic in enumerate(topic_keywords):
            topic_str = ' '.join(topic)
            print(f'Topic: {i}, Keywords: {topic_str}')
            f.write(str(i) + '\t' + topic_str + '\n')
    with open(tp_file,'wb') as f2:
        pickle.dump(tp,f2)


def predict_topics(multilingual_text_file,multilingual_out_file,topic_model_dir,num_topics,num_epochs):
    tp_file = os.path.join(topic_model_dir,'tp.p')
    models_dir = os.path.join(topic_model_dir,'ctm',os.listdir(os.path.join(topic_model_dir,'ctm'))[0])
    print(models_dir)
    with open(tp_file,'rb') as f:
        tp = pickle.load(f)
    print(tp)
    multilingual_documents = load_data(multilingual_text_file)
    prediction_dataset = tp.transform(multilingual_documents)

    ctm = ZeroShotTM(bow_size=len(tp.vocab), contextual_size=768, n_components=num_topics, num_epochs=num_epochs)
    ctm.load(models_dir,epoch=num_epochs-1)
    multilingual_topics_predictions = ctm.get_thetas(prediction_dataset,n_samples=5)
    multilingual_assigned_topics = [str(np.argmax(multilingual_topics_predictions[i])) for i in range(len(multilingual_topics_predictions))]
    
    with open(multilingual_out_file,'w') as f:
        for topic in multilingual_assigned_topics:
            f.write(topic + '\n')


def main():
    num_topics = 50
    num_epochs = 20
    train_size ='1M'
    test_size = 'all'
    base_dir = '/shared/2/projects/cross-lingual-exchange/data/'
    model_dir = '/shared/2/projects/cross-lingual-exchange/models/'
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)
    topic_model_data_dir = os.path.join(base_dir,'topic_model_data')
    english_text_file = os.path.join(topic_model_data_dir,f'english_{train_size}.txt')
    multilingual_text_dir = os.path.join(topic_model_data_dir,'multilingual_with_top100_hashtags')
    multilingual_text_files = os.listdir(multilingual_text_dir)
    multilingual_out_dir = os.path.join(topic_model_data_dir,f'multilingual_with_top100_hashtags_{num_topics}topics_{train_size}train')
    ctm_out_dir = os.path.join(model_dir,f'ctm_{num_topics}topics_{num_epochs}epochs_{train_size}')

    for out_dir in [multilingual_out_dir,ctm_out_dir]:
        if not os.path.exists(out_dir):
            os.mkdir(out_dir)
    for filename in multilingual_text_files:
        print(filename)
        multilingual_text_file = os.path.join(multilingual_text_dir,filename)
        multilingual_out_file = os.path.join(multilingual_out_dir,filename)
        predict_topics(multilingual_text_file,multilingual_out_file,ctm_out_dir,num_topics,num_epochs)

    #outdir = os.path.join(model_dir,f'ctm_{num_topics}topics_{num_epochs}epochs_{train_size}')
    #if not os.path.exists(outdir):
    #    os.mkdir(outdir)
    #tp, training_dataset = prepare_training_dataset(english_text_file)
    #train_ctm(tp,training_dataset,num_topics,num_epochs,outdir)
    #predict_topics(multilingual_text_file,multilingual_out_file,outdir,num_topics,num_epochs)



if __name__ == "__main__":
    main()
