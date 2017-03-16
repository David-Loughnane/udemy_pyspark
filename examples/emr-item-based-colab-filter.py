import sys
from pyspark import SparkConf, SparkContext
from math import sqrt

# conf object is empty, pass parameters on the command line
# when run on master, takes advtange of pre-configured stuff on EMR (whos master, how many client machines, how many executors do they have),
# as well as any command-line options you pass into spark-sumbit from the master node 
# --master local
conf = SparkConf()
sc = SparkContext(conf = conf)


##### instructions to run on EMR #####
# aws s3 cp s3://mydrive-ds-test/david-emr-scripts/emr-item-based-colab-filter.py ./ 
# aws s3 cp s3://sundog-spark/ml-1m/movies.dat ./
# export PYSPARK_PYTHON=python34
# export PYTHONHASHSEED=0
# export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

# star wars is movieID 260
# executor-memory (default is 512MB per executor)
# spark-submit --executor-memory 1g emr-item-based-colab-filter.py 260



def load_movie_names():
	movie_names = {}
	with open("movies.dat", encoding = "ISO-8859-1") as f:
		for line in f:
			fields = line.split('::')
			movie_names[int(fields[0])] = fields[1]
	return movie_names


def parse_ratings(line):
	fields = line.split('::')
	user_id = int(fields[0])
	movie_id = int(fields[1])
	rating = int(fields[2])
	return (user_id, (movie_id, rating))


def make_pairs(user_ratings):
	(movie1, rating1) = user_ratings[1][0]
	(movie2, rating2) = user_ratings[1][1]	
	return ((movie1, movie2),(rating1, rating2))


def filter_duplicates(user_ratings):
	''' Eliminates opposite order and same movie pairs '''
	(movie1, rating1) = user_ratings[1][0]
	(movie2, rating2) = user_ratings[1][1]		
	return movie1 < movie2


def computeCosineSimilarity(rating_pairs):
	num_pairs = 0
	sum_xx = sum_yy = sum_xy = 0
	for rating_x, rating_y, in rating_pairs:
		sum_xx += rating_x * rating_x
		sum_yy += rating_y * rating_y
		sum_xy += rating_x * rating_y
		num_pairs += 1

	numerator = sum_xy
	denominator = sqrt(sum_xx) * sqrt(sum_yy)

	score = 0
	if (denominator):
		score = (numerator / float(denominator))

	return (score, num_pairs)


#############################
#### MAIN PROGRAM BEGINS ####
#############################
name_dict = load_movie_names()

ratings_file = sc.textFile('s3n://sundog-spark/ml-1m/ratings.dat')
#ratings_file = sc.textFile("/Users/davidloughnane/Documents/training/udemy_pyspark/data/ml-1m/ratings.dat")

ratings_rdd = ratings_file.map(parse_ratings)

ratings_rdd_sample = ratings_rdd.sample(withReplacement=False, fraction=0.1, seed=72)

# HUGE RDD, sample above
ratings_rdd_partitioned = ratings_rdd_sample.partitionBy(100)
ratings_pairs_rdd_dup = ratings_rdd_partitioned.join(ratings_rdd)

ratings_pairs_rdd = ratings_pairs_rdd_dup.filter(filter_duplicates)

ratings_only_rdd = ratings_pairs_rdd.map(make_pairs).groupByKey()

# cache the result as we need to use more than once
movie_pair_similarities_rdd = ratings_only_rdd.mapValues(computeCosineSimilarity).cache()
## save results to file
# movie_pair_similarities_rdd.sortByKey()
# each executor will produce it's own file
# movie_pair_similarities_rdd.saveAsTextFile("movie-sims")

# extract similariites for the movie we care about that are 'good'
if len(sys.argv) > 1:
	# how similar
	score_threshold = 0.95
	# how many people 
	co_occurence_threshold = 50
	movie_id = int(sys.argv[1])

	filtered_results_rdd = movie_pair_similarities_rdd.filter(lambda x: (x[0][0] == movie_id or x[0][1] == movie_id) and x[1][0] > score_threshold and x[1][1] > co_occurence_threshold)

	# sort by quality score
	results = filtered_results_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False).take(10)

	print("Top 10 similar movies for {}".format(name_dict[movie_id]))
	for result in results:
		(sim, pair) = result
		# display the similarity result that isn't the movie were comparing against
		similar_movie_id = pair[0]
		if (similar_movie_id == movie_id):
			similar_movie_id = pair[1]
		print(name_dict[similar_movie_id] + '\tscore: ' + str(sim[0]) + '\tstrength: ' + str(sim[1]))





