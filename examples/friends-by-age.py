from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

# rdd with input data
lines = sc.textFile("file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/fakefriends.csv")

# create key:value rdd using parseLine udf
rdd = lines.map(parseLine)

# we need to aggregate - compound operation
# mapValues called first, then reduceByKey
# (33,385) => (33, (385, 1)) mapVALUES !!!
# reduceByKey also only operates on values over the keys - FIRST ACTION
# x = (33, (385, 1)), y = (33, (10, 1)) => (33, (395, 2))
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

# only values are input
# (33, (395, 2)) => (33, 197.5)
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# SECOND ACTION
results = averagesByAge.collect()

for result in results:
    print(result)
