import sys
from pyspark import SparkConf, SparkContext
from pyspark.mllib.recommendation import ALS, Rating


def load_movie_names():
	movie_names = {}
	with open("/Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.ITEM", encoding = "ISO-8859-1") as f:
		for line in f:
			fields = line.split('|')
			movie_names[int(fields[0])] = fields[1]
	return movie_names


conf = SparkConf().setMaster('local[*]').setAppName('MovieReccALS')
sc = SparkContext(conf=conf)
sc.setCheckpointDir('checkpoint/')

name_dict = load_movie_names()
data = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-100k/u.data')

# user, movie, rating
# MLlib is of rating type - actually refers to rating items
# cache - ALS will access this multiple times
ratings = data.map(lambda x: x.split()).map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2]))).cache()

rank = 10
numIterations = 20
ALS.checkpointInterval = 2
model = ALS.train(ratings, rank, numIterations)

userID = int(sys.argv[1])

userRatings = ratings.filter(lambda x: x[0] == userID)
for rating in userRatings.collect():
	print(name_dict[int(rating[1])] + ': ' + str(rating[2]))

recommendations = model.recommendProducts(userID, 10)
print('\nThese are the reccomendations')
for recommendation in recommendations:
	print(name_dict[int(recommendation[1])] + " score " + str(recommendation[2]))