#!/usr/bin/python3

###### SHOULD BE RUN ON PLAIN TEXT ALREADY
###### SAMPLE USAGE #########################
# python3 shrink_corpus.py src_file tgt_file max_segments[number] scope[src,tgt]  update_every
##############################################

import io, re, sys, hashlib, time, unicodedata, datetime
#from itertools import izip
#from itertools import *
#from collections import Counter

def main(argv):
	src_file = argv[0]
	tgt_file = argv[1]
	max_segments = int(argv[2])
	scope = "src"
	if argv[3] not in ['src','tgt']:
		print("The 5th param should be either 'src' or 'tgt'! Aborting...")
		exit(-1)
		#scope=argv[3]
	update_every = 100000
	if len(argv) > 4:
		update_every = int(argv[4])
	cnt_total = 0

	print("====================== SHRINKING CORPORA TO {} SEGMENTS ===================================".format(max_segments))
	print("Source: {}\nTarget: {}\nMax segments: {}\nUpdate Every: {}\nScope: {}".format(src_file,tgt_file,max_segments,update_every,scope))
	start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	start = time.time()
	fwDiscard = io.open("{}.discarded".format(src_file), 'w', encoding='utf-8', newline='\n')
	utable = {} # A new table with seg count: frequencies
	table = {}
	with io.open(src_file,'r',encoding='utf8', newline='\n') as frSrc, io.open(tgt_file,'r',encoding='utf8', newline='\n') as frTgt:
		for src, tgt in zip(frSrc, frTgt): # WE MAY PRUNE RIGHT HERE TO AVOID ADDING USELESS SEGMENTS TO LIST
			#cnt_total=cnt_total+1
			#if cnt_total % update_every == 0:
			#	print("Processed {} lines...".format(cnt_total))
			sgt = ''
			if scope == 'src':
				sgt = src
			elif scope == 'tgt':
				sgt = tgt
			sgt_len = len(sgt.strip().split())
			if sgt_len in table:
				table[sgt_len] = table[sgt_len] + 1
			else:
				table[sgt_len] = 1
	# READ THE FILES
	total = sum(list(table.values())) # total segment count
	print("Total segment count: {}\nTable: {}".format(total,table))
	#ratios = [x/total*100 for x in list(table.values())]
	#print("Ratios: {}".format(ratios))
	scale_out_value = total / max_segments  # we need to shrink by scale_out_value times
	print("Scale-out value: {}".format(scale_out_value))
	for cnt,freq in table.items():
		utable[cnt] = int(freq/scale_out_value) or 1 # If output is less than 1, set to extract 1 segment
	print("New segment count: {}\nNew table: {}".format(sum(list(utable.values())), utable))
	########http://rextester.com/PCPDX7393

	cnt_total = 0
	new_total = 0
	with io.open("{}.filtered".format(src_file), 'w', encoding='utf-8', newline='\n') as  fwFilteredSrc:
		with io.open("{}.filtered".format(tgt_file), 'w', encoding='utf-8', newline='\n') as  fwFilteredTgt:
			with io.open(src_file,'r',encoding='utf8', newline='\n') as frSrc, io.open(tgt_file,'r',encoding='utf8', newline='\n') as frTgt:
				for src, tgt in zip(frSrc, frTgt):
					cnt_total=cnt_total+1
					if cnt_total % update_every == 0:
						print("Processed {} lines...".format(cnt_total))
					sgt = ''
					if scope == 'src':
						sgt = src
					elif scope == 'tgt':
						sgt = tgt
					sgt_len = len(sgt.strip().split())
					if utable[sgt_len]:
						utable[sgt_len] = utable[sgt_len] - 1
						new_total = new_total + 1
						fwFilteredSrc.write(src)
						fwFilteredTgt.write(tgt)
					#else:
					#	fwDiscard.write("{}\t{}\n".format(src.strip(), tgt.strip()))

	end = time.time()
	end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	print("Processed: {} segments.\nNew corpus TUs: {}.\nDiscarded TUs: {}.\nTime spent: {} ({} - {}).".format(cnt_total, new_total, cnt_total-new_total, end - start, start_time, end_time))
	fwDiscard.close()

#	print("Processed: {} segments.".format(cnt_total))
#	print("Dupe segments: {}.".format(cnt_total-cnt_unique))
#	print("Unique segments: {}.".format(cnt_unique))
#	print("Time spent: {} ({} - {}).".format(end - start, start_time, end_time))

	print("====================== CORPUS SHRINKING END ===============================")

if __name__ == "__main__":
	main(sys.argv[1:])


