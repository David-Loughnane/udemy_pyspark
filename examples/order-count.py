from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('orderCount')
sc = SparkContext(conf = conf)


def parse_lines(lines):
	fields = lines.split(',')
	customer_id = int(fields[0])
	order_amount = float(fields[2])
	return (customer_id, order_amount)


lines = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/customer-orders.csv')

orders = lines.map(parse_lines)

totals_rdd = orders.reduceByKey(lambda x,y: x+y)
sorted_totals_rdd = totals_rdd.map(lambda x: (x[1], x[0])).sortByKey(ascending=False)
totals = sorted_totals_rdd.collect()

for i, (total, cust_id) in enumerate(totals):
	if i > 9:
		break
	print("Rank:\t{},\nCust:\t{},\nTotal:\t{:.2f}\n".format(i+1, cust_id, total))