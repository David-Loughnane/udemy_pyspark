from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('MaxTemps')
sc = SparkContext(conf = conf)

def parseLines(lines):
	fields = lines.split(',')
	stationID = fields[0]
	entryType = fields[2]
	temp = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
	return (stationID, entryType, temp)

lines = sc.textFile("file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/1000.csv")

parsedLines = lines.map(parseLines)

# keep only TMAX entry types
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])

# don't need TMAX anymore
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))

# for each station keep only the max temp
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

results = maxTemps.collect()

for result in results:
	print(result[0] + "\t{:.2f}".format(result[1]))