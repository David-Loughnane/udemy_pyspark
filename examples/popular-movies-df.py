from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions



def load_movie_names():
	movie_names = {}
	with open("/Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.ITEM", encoding = "ISO-8859-1") as f:
		for line in f:
			fields = line.split('|')
			movie_names[int(fields[0])] = fields[1]
	return movie_names

spark = SparkSession.builder.appName('PopularMovies').getOrCreate()


name_dict = load_movie_names()
lines = spark.sparkContext.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.data')

# single column called movie id (just trying to find most populatr)
movies_rdd = lines.map(lambda x: Row(movieID = int(x.split()[1])))
movies_df = spark.createDataFrame(movies_rdd)

top_movie_ids = movies_df.groupBy('movieID').count().orderBy('count', ascending=False).cache()

top_movie_ids.show()

top10 = top_movie_ids.take(10)

print('\n')
for result in top10:
	print('{}: {}'.format(name_dict[result[0]], result[1]))

spark.stop()