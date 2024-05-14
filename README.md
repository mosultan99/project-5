## RDD 
# checking the directory
dbutils.fs.ls("/FileStore/tables/")

# COMMAND ----------


dbutils.fs.cp("/FileStore/tables//clinicaltrial_2023.zip", "file:/tmp/")

# COMMAND ----------

dbutils.fs.cp("/FileStore/tables/pharma.zip", "file:/tmp/")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp/ /tmp/clinicaltrial_2023.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC unzip -d /tmp/ /tmp/pharma.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/pharma.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /tmp/clinicaltrial_2023.csv

# COMMAND ----------

#moving the clinical trial 2023 file into a dbfs directory 
dbutils.fs.mkdirs("FileStore/tables/clinicaltrial_2023")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/clinicaltrial_2023.csv", "/FileStore/tables/clinicaltrial_2023", True)

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/clinicaltrial_2023/")

# COMMAND ----------

len(dbutils.fs.ls("FileStore/tables/clinicaltrial_2023/"))

# COMMAND ----------

#moving the pharma file into a dbfs directory 
dbutils.fs.mkdirs("FileStore/tables/pharma")

# COMMAND ----------

dbutils.fs.mv("file:/tmp/pharma.csv", "/FileStore/tables/pharma", True)

# COMMAND ----------

dbutils.fs.ls("FileStore/tables/pharma/")

# COMMAND ----------

len(dbutils.fs.ls("FileStore/tables/pharma/"))

# COMMAND ----------

#taking the content from the clinical trial 2023 dataset
clinical_trial_2023_RDD = sc.textFile("FileStore/tables/clinicaltrial_2023/*")

# COMMAND ----------

clinical_trial_2023_RDD.take(5)

# COMMAND ----------

#taking the content from the pharma dataset
pharma_RDD = sc.textFile("FileStore/tables/pharma/*")

# COMMAND ----------

pharma_RDD.take(5)

# COMMAND ----------

# cleaning clinical trial 2023 dataset
cleanedRDD_clinical = clinical_trial_2023_RDD.map(lambda x: (x.strip('",') .split("\t")))

# COMMAND ----------

cleanedRDD_clinical.take(5)

# COMMAND ----------

cleanedRDD_clinical.first()

# COMMAND ----------

# cleaning pharma dataset
cleanedRDD_pharma = pharma_RDD.map(lambda x: (x.split(",")))

# COMMAND ----------

cleanedRDD_pharma.take(5)

# COMMAND ----------

cleanedRDD_pharma.first()

# COMMAND ----------

# filling in empty spaces  / cleaning data set
no_null_RDD = cleanedRDD_clinical.map(lambda x: x + [" " for i in range( 14 - len(x))])

# COMMAND ----------

# question 1 
Id_RDD = no_null_RDD.map(lambda x: x[0])
Distinct_ID = Id_RDD.distinct()
Distint_ID_count = Distinct_ID.count()
Distint_ID_count

# COMMAND ----------

#question 2
Type_RDD = no_null_RDD.map(lambda x:(x[10], 1))
Type_RDD2 = Type_RDD.reduceByKey(lambda a,b: a + b)

# COMMAND ----------

Type_RDD.take(5)

# COMMAND ----------

Type_RDD2.take(5)

# COMMAND ----------

Clean_Type_RDD = Type_RDD2.filter(lambda x: x[0].strip() != '')

# COMMAND ----------

Clean_Type_RDD.take(5)

# COMMAND ----------

# question 3
from pyspark.sql import Row
from pyspark.sql.functions import split, explode

split_conditions_RDD = no_null_RDD.map(lambda x: x[4].split("|"))

exploded_conditions_RDD = split_conditions_RDD.flatMap(lambda x: x)

condition_count_RDD = exploded_conditions_RDD.map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b)

condition_count_sorted_RDD = condition_count_RDD.sortBy(lambda x: x[1], ascending=False)

condition_count_sorted_RDD.take(5)

# COMMAND ----------

# question 4
parent_company_rdd = cleanedRDD_pharma.map(lambda x:(x[1], 1))
Count_parentcompany  = parent_company_rdd.reduceByKey(lambda a,b: a + b)
Count_parentcompany_desc = Count_parentcompany.sortBy(lambda x: x[1], ascending=False)
Count_parentcompany_desc.take(20)


# COMMAND ----------

sponsor_rdd = no_null_RDD.map(lambda x:(x[6], 1))
Count_sponsor  = sponsor_rdd.reduceByKey(lambda a,b: a + b)
Count_sponsor_desc = Count_sponsor.sortBy(lambda x: x[1], ascending=False)
Count_sponsor_desc.take(10)

# COMMAND ----------

pharma_companies_list = Count_parentcompany_desc.map(lambda x: x[0].strip('"')).collect()
non_pharma_sponsors_rdd = Count_sponsor_desc.filter(lambda x: x[0] not in pharma_companies_list)
non_pharma_sponsors_rdd.take(10)


# COMMAND ----------

# question 5
# Filtering completed studies in the year 2023
completed_2023_rdd = no_null_RDD.filter(lambda x: x[0] == 'COMPLETED' and x[1].startswith('2023'))

# Mapping each row to (month, 1) tuple for counting
month_count_rdd = completed_2023_rdd.map(lambda x: (x[1][5:7], 1))  

# Reducebykey to count the number of completed studies in each month
monthly_counts_rdd = month_count_rdd.reduceByKey(lambda a, b: a + b)

# Collecting the result
monthly_counts_data = monthly_counts_rdd.collect()
monthly_counts_data_RDD = monthly_counts_data

for month, count in monthly_counts_data:
    print(f"Month: {month}, Completed Studies: {count}")

# COMMAND ----------

import calendar
import matplotlib.pyplot as plt
import pandas as pd

# Mapping month numbers to month names
month_names = {i: calendar.month_name[i] for i in range(1, 13)}

# Sample data
data = {
    'Month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    'NumCompletedStudies': [1494, 1272, 1552, 1324, 1415, 1619, 1360, 1230, 1152, 1058, 909, 1082]
}

# Creating a DataFrame from the data
no_null_RDD = pd.DataFrame(data)

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(no_null_RDD['Month'], no_null_RDD['NumCompletedStudies'], color='skyblue')
plt.xlabel('Month')
plt.ylabel('Number of Completed Studies')
plt.title('Number of Completed Studies in 2023 by Month')
plt.xticks(range(1, 13), [month_names[month] for month in range(1, 13)])  # Format x-axis labels
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()




## DataFrame
# Databricks notebook source
#taking the content from the clinical trial 2023 dataset
clinical_trial_2023_RDD = sc.textFile("FileStore/tables/clinicaltrial_2023/*")
clinical_trial_2023_RDD.take(5)

# COMMAND ----------

#taking the content from the pharma dataset
pharma_RDD = sc.textFile("FileStore/tables/pharma/*")
pharma_RDD.take(5)

# COMMAND ----------

# cleaning clinical trial 2023 dataset
cleanedRDD_clinical = clinical_trial_2023_RDD.map(lambda x: (x.strip('",') .split("\t")))
cleanedRDD_clinical.take(5)

# COMMAND ----------

cleanedRDD_clinical.first()

# COMMAND ----------

# cleaning pharma dataset
cleanedRDD_pharma = pharma_RDD.map(lambda x: (x.split(",")))
cleanedRDD_pharma.take(5)

# COMMAND ----------

cleanedRDD_pharma.first()

# COMMAND ----------

# filling in empty spaces  / cleaning dataset clinical trial 
no_null_RDD = cleanedRDD_clinical.map(lambda x: x + [" " for i in range( 14 - len(x))])

# COMMAND ----------

# filling in empty spaces  / cleaning data set pharma
no_null_pharmaRDD = cleanedRDD_pharma.map(lambda x: x + [" " for i in range( 35 - len(x))])

# COMMAND ----------

# creating data frame for the clinical 2023 data 
# Step 1
header = no_null_RDD.first()

# COMMAND ----------

# Step 2
data = no_null_RDD.filter(lambda row: row != header)

# COMMAND ----------

# Step 3
clinical_df = data.toDF(header)

# COMMAND ----------

display(clinical_df)

# COMMAND ----------

clinical_df.printSchema()

# COMMAND ----------

# Convert the data pharma RDD to DF
pharma_df = spark.read.option("delimiter", ",").csv("/FileStore/tables/pharma/",header=True)

# COMMAND ----------

display(pharma_df)

# COMMAND ----------

pharma_df.printSchema()

# COMMAND ----------

# question 1
# Get the number of studies in the dataset
num_studies = clinical_df.count()


# COMMAND ----------

# Print the number of studies
print("Number of studies in the dataset:", num_studies)

# COMMAND ----------

# question 2
from pyspark.sql import functions as F

# Group by the Type column and count the frequency 
type_counts_df = clinical_df.groupBy("Type").count()

# Order the DataFrame by count in descending order to get the most frequent types first
type_counts_sorted_df = type_counts_df.orderBy(F.desc("count"))


type_counts_sorted_df.show()

# COMMAND ----------

# question 3
from pyspark.sql import functions as F

# Step 1
conditions_df = clinical_df.select(F.explode(F.split("Conditions", "\|")).alias("Condition"))

# Step 2
condition_counts_df = conditions_df.groupBy("Condition").count()

# Step 3
condition_counts_sorted_df = condition_counts_df.orderBy(F.desc("count"))

# Step 4
top_5_conditions_df = condition_counts_sorted_df.limit(5)


top_5_conditions_df.show()

# COMMAND ----------

# question 4
from pyspark.sql.functions import col

# Extracting the list of pharmaceutical companies
pharma_companies = pharma_df.select("Parent_Company").rdd.flatMap(lambda x: x).collect()

# Filtering out sponsors that are not pharmaceutical companies
filtered_clinical_df = clinical_df.filter(~col("Sponsor").isin(pharma_companies))

# Counting the frequency of each sponsor
sponsor_counts = filtered_clinical_df.groupBy("Sponsor").count()

# Selecting the top 10 sponsors 
top_10_sponsors = sponsor_counts.orderBy(col("count").desc()).limit(10)


top_10_sponsors.show()

# COMMAND ----------

# question 5
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month

# Creating a SparkSession
spark = SparkSession.builder \
    .appName("CompletedStudiesAnalysis") \
    .getOrCreate()

# Filtering completed studies in the year 2023
completed_2023_df = clinical_df.filter((year("Completion") == 2023) & (clinical_df["Status"] == "COMPLETED"))

# Grouping by month and counting the number of completed studies
monthly_counts_df = completed_2023_df.groupBy(month("Completion").alias("Month")) \
    .count() \
    .orderBy("Month")


monthly_counts_df.show()

# COMMAND ----------

import calendar
import matplotlib.pyplot as plt
import pandas as pd

# Mapping month numbers to month names
month_names = {i: calendar.month_name[i] for i in range(1, 13)}

# Sample data
data = {
    'Month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
    'NumCompletedStudies': [1494, 1272, 1552, 1324, 1415, 1619, 1360, 1230, 1152, 1058, 909, 1082]
}

# Creating a DataFrame from the data
clinical_df = pd.DataFrame(data)

# Plotting
plt.figure(figsize=(10, 6))
plt.bar(clinical_df['Month'], clinical_df['NumCompletedStudies'], color='skyblue')
plt.xlabel('Month')
plt.ylabel('Number of Completed Studies')
plt.title('Number of Completed Studies in 2023 by Month')
plt.xticks(range(1, 13), [month_names[month] for month in range(1, 13)])  # Format x-axis labels
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()




## SQL 
-- Databricks notebook source
-- MAGIC %python
-- MAGIC #taking the content from the clinical trial 2023 dataset
-- MAGIC clinical_trial_2023_RDD = sc.textFile("FileStore/tables/clinicaltrial_2023/*")
-- MAGIC clinical_trial_2023_RDD.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #taking the content from the pharma dataset
-- MAGIC pharma_RDD = sc.textFile("FileStore/tables/pharma/*")
-- MAGIC pharma_RDD.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # cleaning clinical trial 2023 dataset
-- MAGIC cleanedRDD_clinical = clinical_trial_2023_RDD.map(lambda x: (x.strip('",') .split("\t")))
-- MAGIC cleanedRDD_clinical.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cleanedRDD_clinical.first()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # cleaning pharma dataset
-- MAGIC cleanedRDD_pharma = pharma_RDD.map(lambda x: (x.split(",")))
-- MAGIC cleanedRDD_pharma.take(5)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC cleanedRDD_pharma.first()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # filling in empty spaces  / cleaning data set
-- MAGIC no_null_RDD = cleanedRDD_clinical.map(lambda x: x + [" " for i in range( 14 - len(x))])

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # creating data frame for the clinical 2023 data 
-- MAGIC # Step 1: Extract the header from the RDD
-- MAGIC header = no_null_RDD.first()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Step 2: Remove the header from the RDD to get the data
-- MAGIC data = no_null_RDD.filter(lambda row: row != header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Step 3: Convert the data RDD into a DataFrame using the extracted header
-- MAGIC clinical_df = data.toDF(header)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Display the DataFrame
-- MAGIC display(clinical_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinical_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Convert the data RDD into a DataFrame using the extracted header
-- MAGIC pharma_df = spark.read.option("delimiter", ",").csv("/FileStore/tables/pharma/",header=True)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Display the DataFrame
-- MAGIC display(pharma_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df.printSchema()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC clinical_df . createOrReplaceTempView ("sqlclinical")

-- COMMAND ----------

select * from sqlclinical limit 10

-- COMMAND ----------

-- MAGIC %python
-- MAGIC pharma_df . createOrReplaceTempView ("sqlpharma")

-- COMMAND ----------

select * from sqlpharma limit 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC question 1

-- COMMAND ----------

SELECT COUNT('Id') AS TotalStudies
FROM sqlclinical

-- COMMAND ----------

-- MAGIC %md
-- MAGIC question 2

-- COMMAND ----------

SELECT Type, COUNT(*) AS Frequency
FROM sqlclinical
GROUP BY Type
ORDER BY Frequency DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC question 3

-- COMMAND ----------

SELECT Conditions, COUNT(*) AS Frequency
FROM sqlclinical
WHERE Conditions IS NOT NULL
GROUP BY Conditions
ORDER BY Frequency DESC
LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC question 4

-- COMMAND ----------

SELECT Sponsor, COUNT(*) AS Frequency
FROM sqlclinical
WHERE Sponsor NOT IN (SELECT Parent_Company FROM sqlpharma)
GROUP BY Sponsor
ORDER BY Frequency DESC
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC QUESTION 5

-- COMMAND ----------

SELECT MONTH(Completion) AS Month,
       COUNT(*) AS NumCompletedStudies
FROM sqlclinical
WHERE YEAR(Completion) = 2023
      AND Status = 'COMPLETED'
GROUP BY MONTH(Completion)
ORDER BY MONTH(Completion);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import calendar
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import pandas as pd
-- MAGIC
-- MAGIC # Mapping month numbers to month names
-- MAGIC month_names = {i: calendar.month_name[i] for i in range(1, 13)}
-- MAGIC
-- MAGIC # Sample data
-- MAGIC data = {
-- MAGIC     'Month': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
-- MAGIC     'NumCompletedStudies': [1494, 1272, 1552, 1324, 1415, 1619, 1360, 1230, 1152, 1058, 909, 1082]
-- MAGIC }
-- MAGIC
-- MAGIC # Creating a DataFrame from the data
-- MAGIC sqlclinical = pd.DataFrame(data)
-- MAGIC
-- MAGIC # Plotting
-- MAGIC plt.figure(figsize=(10, 6))
-- MAGIC plt.bar(sqlclinical['Month'], sqlclinical['NumCompletedStudies'], color='skyblue')
-- MAGIC plt.xlabel('Month')
-- MAGIC plt.ylabel('Number of Completed Studies')
-- MAGIC plt.title('Number of Completed Studies in 2023 by Month')
-- MAGIC plt.xticks(range(1, 13), [month_names[month] for month in range(1, 13)])  # Format x-axis labels
-- MAGIC plt.grid(axis='y', linestyle='--', alpha=0.7)
-- MAGIC plt.show()
