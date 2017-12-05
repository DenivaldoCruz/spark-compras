import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
#from pyspark import Row
import time
import sys
 
start_time = time.time()

conf = SparkConf().setAppName("Spark Compras")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)

filename = sys.argv[1]
print("File path", filename)
compras = sc.textFile(filename) \
    .map(lambda line: line.split(";")) \
    .filter(lambda line: len(line)>1) \
    .map(lambda line: (line[2], line[3])) \
    #.repartition(1)
    #.collect()
    

counts = compras.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

group = counts.map(lambda x: (x[0][0], (x[1], x[0][1]))).reduceByKey(max)

sorted = group.sortBy(lambda x:x[0], True)

#counts.saveAsTextFile("hdfs://input/maisVendidoPorCidade")
sorted.collect()
sorted.saveAsTextFile("produto_mais_vendido_por_cidade_rdd")

end_time = time.time()

duration = end_time - start_time
print('Duration ', duration)
