from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('mostPopularSuperhero')
sc = SparkContext(conf=conf)


names_file = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/Marvel-Names.txt')
graph_file  = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/Marvel-Graph.txt')

def parse_hero_names(line):
	split_line = line.split('\"')
	return (int(split_line[0]), split_line[1].encode("utf8"))

def parse_co_occurences(line):
	split_line = line.split()
	return (int(split_line[0]), len(split_line) - 1)

# names stored as k:v RDD instead of broadcast dict
names_rdd = names_file.map(parse_hero_names)
pairings_rdd = graph_file.map(parse_co_occurences)

friends_by_heroID = pairings_rdd.reduceByKey(lambda x,y: x+y).map(lambda x: (x[1], x[0]))

# maximum KEY value
most_popular_heroID = friends_by_heroID.max()

# list that contains single string, use [0] to get string
most_popular_hero_name = names_rdd.lookup(most_popular_heroID[1])[0]

print('Most popular superhero: {}, with {} co-occurrences'.format(most_popular_hero_name, most_popular_heroID[0]))

