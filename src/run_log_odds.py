import os
import sys
import glob
import ujson as json
from collections import defaultdict,Counter
from log_odds import LoadCounts, ComputeLogOdds 


def normalize(counts1,counts2):
	max1 = max(counts1.values())
	max2 = max(counts2.values())

	if max1 > max2:
		ratio = max1/max2
		for key in counts2:
			counts2[key] *= ratio 

	if max2 > max1:
		ratio = max2/max1
		for key in counts1:
			counts1[key] *= ratio
	
	new_prior = defaultdict(int)
	for key in counts1:
		new_prior[key] += counts1[key]
	for key in counts2:
		new_prior[key] += counts2[key]
		
	return new_prior 

def subtract_counts(prior,counts1):
	counts2 = {}
	for key in prior:
		counts2[key] = prior[key]
		if key in counts1:
			counts2[key] -= counts1[key]
	return counts2
	  

def main():

	
    
	
		#info_type = 'domains' # 'domains'
	#info_type = 'hashtags' # 'domains' or  temporal hashtags
	#data_path = f"/shared/2/projects/cross-lingual-exchange/data/{info_type}_no_rt/"

	data_path = "/shared/2/projects/cross-lingual-exchange/data/temporal_hashtags_no_rt"
	for interval in os.listdir(data_path):
		interval_dir = os.path.join(data_path,interval)
		print(interval_dir)
		comparison_unit = 'language' #'language'
		count_path = os.path.join(interval_dir,comparison_unit)
		full_count_file = os.path.join(count_path,'full_count','full_count.json')
		out_path = os.path.join(count_path,'log_odds')	
		if not os.path.exists(out_path):
			os.mkdir(out_path)


		with open(full_count_file,'r') as f:
			full_count = json.load(f)

		most_common = [w[0] for w in Counter(full_count).most_common(200)]
		for i,w in enumerate(most_common):
			print(i,w)

			
		all_files = glob.glob(os.path.join(count_path,'*.json'))
		prior = LoadCounts(full_count_file,min_count=10,stopwords=set(most_common))

		for i,filename in enumerate(all_files):
			print(i,filename) 
			try:
				counts1 = LoadCounts(filename,min_count=10,stopwords=most_common)
				counts2 = subtract_counts(prior,counts1)
				prior_norm = normalize(counts1,counts2)
				delta = ComputeLogOdds(counts1, counts2, prior_norm)
				out_file = os.path.join(out_path,os.path.basename(filename).split('.')[0] + '.tsv')
				with open(out_file,'w') as f:
					for word, log_odds in sorted(delta.items(), key=lambda x: x[1],reverse=True):
						f.write("{}\t{:.3f}\n".format(word, log_odds))
			except:
				continue

			
		



if __name__ == "__main__":
	main()

