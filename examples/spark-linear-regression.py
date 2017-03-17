from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession 
from pyspark.ml.linalg import Vectors


if __name__ == "__main__":
	# Create a SparkSession
	spark = SparkSession.builder.appName('LinearRegression').getOrCreate()

	inputLines = spark.sparkContext.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/regression.txt')
	data = inputLines.map(lambda x: x.split(",")).map(lambda x: (float(x[0]), Vectors.dense(float(x[1]))))

	colNames = ['label', 'features']
	df = data.toDF(colNames)

	trainTest = df.randomSplit([0.8,0.2])
	trainingDF = trainTest[0]
	testDF = trainTest[1]

	lin_reg = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
	model = lin_reg.fit(trainingDF)
	fullPredictions = model.transform(testDF).cache()
	predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
	labels = fullPredictions.select('label').rdd.map(lambda x: x[0])

	predictionAndLabel = predictions.zip(labels).collect()

	for prediction in predictionAndLabel:
		print(prediction)


