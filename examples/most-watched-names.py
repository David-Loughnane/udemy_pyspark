from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('mostWatchedNames')
sc = SparkContext(conf = conf)


def load_movie_names():
	movie_names = {}
	with open("/Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.ITEM", encoding = "ISO-8859-1") as f:
		for line in f:
			fields = line.split('|')
			movie_names[int(fields[0])] = fields[1]
	return movie_names

nameDict = sc.broadcast(load_movie_names())

lines_rdd = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.data')

movies_rdd = lines_rdd.map(lambda x: (int(x.split()[1])))
movies_count_rdd = movies_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

movie_names_count_rdd = movies_count_rdd.map(lambda x: (nameDict.value[x[1]], x[0]))

movie_names_count = movie_names_count_rdd.collect()

for i, (name, views) in enumerate(movie_names_count):
	if i > 9:
		break
	print("Rank:\t{},\nMovie:\t{},\nViews:\t{}\n".format(i+1, name, views))