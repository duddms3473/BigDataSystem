#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# 과제 8


# In[ ]:


peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
peopleDF.show(5)


# In[ ]:


peopleDF.select('gender', 'salary').show(5)


# In[ ]:


from pyspark.sql.functions import year
peopleDF = peopleDF.select('lastName', 'salary', 'birthDate').filter("gender='M'" and year("birthDate")> "1990")
peopleDF.show(5)


# In[ ]:


#과제 9


# In[ ]:


peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
from pyspark.sql.functions import col
dordonDF = peopleDF.              select(year("birthDate").alias("birthYear"), "firstName").               filter((col("firstName")=='Donna') | (col("firstName")=='Dorothy')).              filter("gender == 'F'").              groupBy('birthYear', 'firstName').              count()
dordonDF.show(5)


# In[ ]:


peopleDF = spark.read.parquet("/mnt/training/dataframes/people-10m.parquet")
from pyspark.sql.functions import col
dordonDF = peopleDF.              select(year("birthDate").alias("birthYear"), "firstName").               filter((col("firstName")=='Donna') | (col("firstName")=='Dorothy')).              filter("gender == 'F'").              groupBy('birthYear', 'firstName').              count()

dordonDF.orderBy('birthYear').show(5)


# In[ ]:


dordonDF = peopleDF.              select(year("birthDate").alias("birthYear"), "firstName").               filter((col("firstName")=='Donna') | (col("firstName")=='Dorothy')).              filter("gender == 'F'").              filter(year("birthdate")>1990).              groupBy('birthYear', 'firstName').              count()

dordonDF = dordonDF.orderBy('birthYear')


# In[ ]:


display(dordonDF)


# In[ ]:


from pyspark.sql.functions import count, desc
top10MaleFirstNameDF = peopleDF.        select("firstName").        filter("gender = 'M'").        groupBy("firstName").        agg(count(col('firstName')).alias('total')).        orderBy(desc('total')).        limit(10)

top10MaleFirstNameDF.show(5)


# In[ ]:


top10MaleFirstNameDF.createOrReplaceTempView("top10MaleFirstName")
resultsDF = spark.sql("select * from top10MaleFirstName order by firstName")
display(resultsDF)


# In[ ]:


# 과제 10


# In[ ]:


bikeSharingDayDF = (spark
  .read                                                
  .option("inferSchema","true")                       
  .option("header","true")                             
  .csv("/mnt/training/bikeSharing/data-001/day.csv"))  
bikeSharingDayDF.show(3)


# In[ ]:


databricksBlogDF = spark.read                        .option("inferSchema","true")                        .option("header","true")                        .json("/mnt/training/databricks-blog.json")


# In[ ]:


databricksBlogDF.printSchema()


# In[ ]:


from pyspark.sql.functions import lower, month, col, lit, date_format,to_date
BlogDF2 = databricksBlogDF.select('authors', 'categories', 'creator', 'dates')                          .withColumn("publishedOn",date_format(col("dates.publishedOn"), "yyyy"))
BlogDF2.show(3)


# In[ ]:


resultDF = databricksBlogDF.select('categories', 'creator', to_date(col("dates.publishedOn")).alias('date'))                            .filter(year(col("date"))=='2013')                            .orderBy(col("date"))

resultDF.show(3)


# In[ ]:


from pyspark.sql.functions import explode, size
databricksBlog2DF = databricksBlogDF.  select('title', 'authors', explode(col('authors')).alias('author')).  filter(size(col('authors'))>1).  orderBy('title')
databricksBlog2DF.show(5)


# In[ ]:


# 과제 11


# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS fireIncidents
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-incidents/fire-incidents-2016.csv",
  inferSchema "true"
)


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT `Incident Number` 
FROM fireIncidents 
WHERE `Incident Date` = '04/04/2016' or `Incident Date` = '09/27/2016'


# In[ ]:


get_ipython().run_line_magic('sql', '')
select `Ignition Cause`, count(`Ignition Cause`) as count 
from fireIncidents
GROUP BY `Ignition Cause`
ORDER BY count DESC


# In[ ]:


get_ipython().run_line_magic('sql', '')
CREATE TABLE IF NOT EXISTS fireCalls
USING csv
OPTIONS (
  header "true",
  path "/mnt/davis/fire-calls/fire-calls-truncated-comma.csv",
  inferSchema "true"
)


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT count(*)
FROM fireIncidents


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT count(*)
FROM fireCalls


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT /*+BROADCAST(fireCalls)*/ *
FROM fireCalls
JOIN fireIncidents on fireCalls.`Battalion` = fireIncidents.`Battalion`


# In[ ]:


get_ipython().run_line_magic('sql', '')
SELECT /*+BROADCAST(fireIncidents)*/ *
FROM fireIncidents
JOIN fireCalls on fireCalls.`Battalion` = fireIncidents.`Battalion`


# In[ ]:




