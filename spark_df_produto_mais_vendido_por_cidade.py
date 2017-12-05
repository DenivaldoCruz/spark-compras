
# coding: utf-8

# In[1]:


import findspark
findspark.init('/home/ubuntu/spark-2.1.1-bin-hadoop2.7')


# In[100]:



from pyspark import SparkContext
from pyspark.sql import SparkSession
import time
import sys
from pyspark.sql.functions import col, count, max


# In[3]:

start_time = time.time()

spark = SparkSession.builder.appName("Python Spark Agg Compras").getOrCreate()


# In[4]:



# spark is an existing SparkSession
filename = sys.argv[1]
df = spark.read.option("delimiter", ";").csv(filename, inferSchema = True, header = True)


# In[5]:


#df.show()


# In[75]:


group = df.groupBy(['Cidade', 'Produto'])


# In[143]:


count_prod = group.count()


# In[109]:





# In[138]:


max_prod = count_prod.groupBy('Cidade').max('count').select(col('Cidade'), col("max(count)").alias("count"))


# In[136]:


#max_prod.show()


# In[147]:


df2 = count_prod.join(max_prod, ['Cidade', 'count'])


# In[148]:


order = df2.orderBy('Cidade')


# In[149]:

order.coalesce(1).write.option("header", "true").csv("produto_mais_vendido_por_cidade_df")

end_time = time.time()

duration = end_time - start_time
print('Duration ', duration)

