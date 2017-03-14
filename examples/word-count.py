# map Vs flatmap
from pyspark import SparkConf, SparkContext
import re
import collections
from operator import itemgetter
import time

conf = SparkConf().setMaster('local').setAppName('wordCount')
sc = SparkContext(conf = conf)

'''
lines = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/quick-brown.txt')

# MAP - 1 to 1 mapping
rageCaps = lines.map(lambda x: x.upper())

# FLATMAP - RDD has more elements than start
words = lines.flatMap(lambda x: x.split())

result1 = rageCaps.collect()
result2 = words.collect()

for result in result1:
	print(result, ' ')

for result in result2:
	print(result, ' ')
'''

def normalise_words(text):
	''' regex - break up text based on words (strip out punctuation), transform all to lower case'''
	return re.compile(r'\W+', re.UNICODE).split(text.lower())

book = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/book.txt')


time1 = time.time()

words = book.flatMap(normalise_words)
word_count = words.countByValue()

# sort on the value of the dict, order from largest to smallest
# this sorts on the python object, we want to sort on the RDD
sorted_word_count = collections.OrderedDict(sorted(word_count.items(), key=itemgetter(1), reverse=True))

print('Sorted Python Object\n')
for i, (word, count) in enumerate(sorted_word_count.items()):
	if i == 20:
		break
	clean_word = word.encode('ascii', 'ignore')
	print('{0}: {1}'.format(word, count))

time2 = time.time()




time3 = time.time()
# implement countByValue
word_count_rdd = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)
# flip (word. count) tp (count, word) pairs, then sort by key
word_counts_sorted_rdd = word_count_rdd.map(lambda x: (x[1],x[0])).sortByKey(ascending=False)
# bring rdd into python object
word_counts_sorted = word_counts_sorted_rdd.collect()

print('\nSorted RDD\n')
for i, (count, word) in enumerate(word_counts_sorted):
	if i == 20:
		break
	clean_word = word.encode('ascii', 'ignore')
	print('{0}: {1}'.format(word, count))

time4 = time.time()

print('\n{} for python object sort, {} for rdd sort'.format(time2-time1, time4-time3))


