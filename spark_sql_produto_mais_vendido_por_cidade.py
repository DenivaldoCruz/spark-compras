
# coding: utf-8

# In[26]:


import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')


# In[25]:



from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
import sys


# In[10]:

start_time = time.time()

spark = SparkSession.builder.appName("Python Spark DataFrame Compras").getOrCreate()


# In[28]:



# spark is an existing SparkSession
filename = sys.argv[1]
df = spark.read.format("csv").option("header", "true").option("delimiter", ";").load(filename)


# In[29]:


df.createOrReplaceTempView("Compras")


# In[30]:


# SQL can be run over DataFrames that have been registered as a table.
compras = spark.sql("SELECT Cidade FROM Compras")


# In[31]:


comprasAgrupadasPorCidadeEProduto = spark.sql("SELECT Cidade, Produto, SUM(Valor) AS Total FROM Compras GROUP BY Cidade, Produto ORDER BY Cidade, Total DESC")


# In[32]:


# Creates a temporary view using the DataFrame
comprasAgrupadasPorCidadeEProduto.createOrReplaceTempView("ComprasAgrupadas")


# In[33]:


produtosMaisVendidosPorCidade = spark.sql("SELECT Cidade, Produto, Total FROM ComprasAgrupadas T1 WHERE EXISTS (SELECT 1 AS Total FROM ComprasAgrupadas T2 WHERE T2.Cidade = T1.Cidade GROUP BY Cidade HAVING MAX(T2.Total) = T1.Total)")


# In[22]:


produtosMaisVendidosPorCidade.coalesce(1).write.option("header", "true").csv("produto_mais_vendido_por_cidade_sql")


# In[36]:


#produtosMaisVendidosPorCidade.show(100)

end_time = time.time()

duration = end_time - start_time
print('Duration ', duration)

