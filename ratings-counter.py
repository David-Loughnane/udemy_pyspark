from pyspark import SparkConf, SparkContext
import collections

# telling spark to execute on the local machine
# app name allows you to view in the spark web UI
conf = SparkConf().setMaster('local').setAppName('RatingsHistogram')
sc = SparkContext(conf = conf)

# load the data from local file into a RDD
lines = sc.textFile("file:///Users/davidloughnane/Documents/training/udemy_pyspark/ml-100k/u.data")

# transform the lines RDD (doesn't force evaluation, not in place) to extract movie ratines
ratings = lines.map(lambda x: x.split()[2])
# apply action - generates DAG, forces evaluation, returns python object (not RDD)
result = ratings.countByValue()

# python collections module to sort by movie rating
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
	print('{0}: {1}'.format(key, value))