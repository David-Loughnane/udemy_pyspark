from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MinTemps')
sc = SparkContext(conf = conf)

def parseLines(lines):
	fields = lines.split(',')
	stationID = fields[0]
	entryType = fields[2]
	temp = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
	return (stationID, entryType, temp)

lines = sc.textFile("file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/1000.csv")

parsedLines = lines.map(parseLines)

# keep only TMIN entry types
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])

# don't need TMIN anymore
stationTemps = minTemps.map(lambda x: (x[0], x[2]))


minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

results = minTemps.collect()

for result in results:
	print(result[0] + "\t{:.2f}".format(result[1]))