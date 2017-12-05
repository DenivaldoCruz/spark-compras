from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import Row
 
sc = SparkContext()
spark = SparkSession(sc)

input = sc.textFile("hdfs://input/itensMaisVendidoPorCidade")
#textFile = sc.textFile("compras.txt")

compras = input.map(lambda line: line.split(";")) \
    .filter(lambda line: len(line)>1) \
    .map(lambda line: (line[2], line[3])) 
    #.collect()
    

#compras = input.flatMap(lambda line: line.split(";"))
counts = compras.map(lambda x: (x[0] + '-' + x[1], 1)).reduceByKey(lambda a, b: a + b)

counts2 = counts.map(lambda line: (line[0].split("-")[0], str(line[1]) + '-' + line[0].split("-")[1])) 
counts2 = counts2.map(lambda x: (x[0], x[1])).reduceByKey(max)
counts2.sortBy(lambda x: x,False).take(10)
#counts.saveAsTextFile("hdfs://input/maisVendidoPorCidade")
counts2.saveAsTextFile("hdfs://input/itensMaisVendidoPorCidade")
