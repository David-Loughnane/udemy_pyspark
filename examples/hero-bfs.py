from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName('heroBFS')
sc = SparkContext(conf=conf)



names_file = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/Marvel-Names.txt')
graph_file  = sc.textFile('file:///Users/davidloughnane/Documents/training/udemy_pyspark/data/Marvel-Graph.txt')


def parse_hero_names(line):
	split_line = line.split('\"')
	return (int(split_line[0]), split_line[1].encode("utf8"))

def create_bfs_graph(line, startNode):
	split_line = line.split()
	node = int(split_line[0])
	connections = []
	for connection in split_line[1:]:
		connections.append(int(connection))
	
	colour = 'WHITE'
	distance = 9999

	if (node == startNode):
		colour = 'GREY'
		distance = 0

	return (node, (connections, distance, colour))


names_rdd = names_file.map(parse_hero_names)
graph_rdd = graph_file.map(lambda x: create_bfs_graph(x, 10))

graph = graph_rdd.collect()

for node in graph:
	print(node, '\n')