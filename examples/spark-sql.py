from pyspark.sql import SparkSession
from pyspark.sql import Row
import collections

# gives us spark context and sql context
spark = SparkSession.builder.appName('SparkSQL').getOrCreate()



def parse_lines(line):
	''' create row objects '''
	fields = line.split(',')
	# ID, name, age, and numFriends columns that are typed -> greater optimisation
	return Row(ID=int(fields[0]), name=fields[1], age=int(fields[2]), numFriends=int(fields[3]))

# rdd - data is not structured to begin with
lines_rdd = spark.sparkContext.textFile('/Users/davidloughnane/Documents/training/udemy_pyspark/data/fakefriends.csv')
# impart structure on the rdd (df is rdd of row objects)
people_rdd = lines_rdd.map(parse_lines)

# turn rdd of row object to df offically, infer schema, register df as table, and cache as we're going to re-use it
schemaPeople_df = spark.createDataFrame(people_rdd).cache()
# creates a temp sql table in memory
schemaPeople_df.createOrReplaceTempView('people')

#all = spark.sql("SELECT ID, name, FROM people")
teenagers = spark.sql("SELECT name, age, numFriends FROM people WHERE age >= 13 AND age <= 19 ORDER BY AGE, numFriends")

# results of SQL queries are RDDs and support all the normal RDD operations
for teen in teenagers.collect():
	print(teen)

# we can also use functions instead of SQL queries (arguably more efficient)
schemaPeople_df.groupBy('age').count().orderBy('age').show()


The master node does not have large computational requirements. For most clusters of 50 or fewer nodes, consider using a m3.xlarge . For clusters of more than 50 nodes, consider using an m3.2xlarge.