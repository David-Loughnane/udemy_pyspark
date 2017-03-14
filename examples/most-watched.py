from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('mostWatchedMovie')
sc = SparkContext(conf = conf)


def parse_movies(lines):
	line = lines.split()
	movie = line[1]
	return movie

lines = sc.textFile("file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.data")


movies_rdd = lines.map(parse_movies)

#movies_count_rdd = movies_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
#sorted_movies_count_rdd = movies_count_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
movies_count_rdd = movies_rdd.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0])).sortByKey(ascending=False)

sorted_movies_count = movies_count_rdd.collect()

for i, (views, movie_id) in enumerate(sorted_movies_count):
	if i > 9:
		break
	print("Rank:\t{},\nMovie:\t{},\nViews:\t{}\n".format(i+1, movie_id, views))