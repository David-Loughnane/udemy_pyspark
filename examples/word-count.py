# map Vs flatmap
from pyspark import SparkConf, SparkContext
import collections
from operator import itemgetter

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


book = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/book.txt')

words = book.flatMap(lambda x: x.split())
word_count = words.countByValue()
sorted_word_count = collections.OrderedDict(sorted(word_count.items(), key=itemgetter(1), reverse=True))


for word, count in sorted_word_count.items():
	clean_word = word.encode('ascii', 'ignore')
	print('{0}: {1}'.format(word, count))




