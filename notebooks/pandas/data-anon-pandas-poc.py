# Databricks notebook source
# MAGIC %md
# MAGIC # data-anon-pandas-poc

# COMMAND ----------

# MAGIC %python
# MAGIC display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

# COMMAND ----------

import pandas as pd
import hashlib

# Define data frame. Bear in mind this could be changed to read_sql
df = pd.read_csv("/dbfs/FileStore/tables/uk_500-626ee.csv", delimiter=',', header=0)
# Sort values by first_name column
df.sort_values("first_name", axis=0, ascending=True, inplace=True, na_position='last')
# Select just the first_name and last_name columns. Drop others and define new dataframe df2.
df2 = df[['first_name','last_name']].dropna()
# output the data
print(df2.head())

# hash first_name limit 15
df2['first_name_hash'] = [hashlib.sha1(str.encode(str(i))).hexdigest()[:15] for i in df2['first_name']]
# hash last_name limit 15
df2['last_name_hash'] = [hashlib.sha1(str.encode(str(i))).hexdigest()[:15] for i in df2['last_name']]
# output the data
print(df2.head())

df3 = df2[['first_name_hash','last_name_hash']]
print(df3.head())

df4 = df3[['first_name_hash','last_name_hash']].rename(columns={'first_name_hash':'first_name','last_name_hash':'last_name'})
print(df4.head())


# COMMAND ----------

display(sql("select * from default.uk_500_626ee_csv order by first_name limit 5"))

# COMMAND ----------
