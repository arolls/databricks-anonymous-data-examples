-- Databricks notebook source
-- MAGIC %md
-- MAGIC # data-anon-faker-poc

-- COMMAND ----------

-- MAGIC %sh pip install Faker unicodecsv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import csv
-- MAGIC from faker import Factory
-- MAGIC from faker.providers import *
-- MAGIC from collections import defaultdict
-- MAGIC 
-- MAGIC def anonymize_rows(rows):
-- MAGIC     # Rows is an iterable of dictionaries that contain name and email fields that need to be anonymized.
-- MAGIC     # Load faker
-- MAGIC     f = Factory.create('en_GB')
-- MAGIC 
-- MAGIC     # Create mappings of rows to faked data.
-- MAGIC     first_names    = defaultdict(f.first_name)
-- MAGIC     last_names     = defaultdict(f.last_name)
-- MAGIC     company_names  = defaultdict(f.company)
-- MAGIC     house_number   = defaultdict(f.building_number)
-- MAGIC     street_name    = defaultdict(f.street_name)
-- MAGIC     cities         = defaultdict(f.city)
-- MAGIC     counties       = defaultdict(f.city_suffix)
-- MAGIC     postals        = defaultdict(f.postcode)
-- MAGIC     phones1        = defaultdict(f.msisdn)
-- MAGIC     phones2        = defaultdict(f.phone_number)
-- MAGIC     emails         = defaultdict(f.email)
-- MAGIC 
-- MAGIC     # Iterate over the rows from the file and yield anonymized rows.
-- MAGIC     for row in rows:
-- MAGIC         # Replace first_name with faked fields.
-- MAGIC         row["first_name"]   = first_names[row["first_name"]]
-- MAGIC         row["last_name"]    = last_names[row["last_name"]]
-- MAGIC         row["company_name"] = company_names[row["company_name"]]
-- MAGIC         row["address"]      = house_number[row["address"]] + " " + street_name[row["address"]]
-- MAGIC         row["city"]         = cities[row["city"]]
-- MAGIC         row["county"]       = counties[row["county"]]
-- MAGIC         row["postal"]       = postals[row["postal"]]
-- MAGIC         row["phone1"]       = phones1[row["phone1"]]
-- MAGIC         row["phone2"]       = phones2[row["phone2"]]
-- MAGIC         row["email"]        = emails[row["email"]]
-- MAGIC 
-- MAGIC         # Yield the row back to the caller
-- MAGIC         yield row
-- MAGIC 
-- MAGIC def anonymize(source, target):
-- MAGIC     #The source argument is a path to a CSV file containing data to anonymize, while target is a path to write the anonymized CSV data to.
-- MAGIC     with open(source, newline='') as csvin:
-- MAGIC         with open(target, 'w') as csvout:
-- MAGIC             # Use the DictReader to easily extract fields
-- MAGIC             reader = csv.DictReader(csvin)
-- MAGIC             writer = csv.DictWriter(csvout, reader.fieldnames)
-- MAGIC 
-- MAGIC             # Read and anonymize data, writing to target file.
-- MAGIC             for row in anonymize_rows(reader):
-- MAGIC                 writer.writerow(row)
-- MAGIC 
-- MAGIC anonymize("/dbfs/FileStore/tables/uk_500-626ee.csv", "/dbfs/FileStore/tables/uk_500-626ee-anonymized.csv")

-- COMMAND ----------

DROP TABLE IF EXISTS data_table;

CREATE TABLE `data_table` (`first_name` STRING, `last_name` STRING, `company_name` STRING, `address` STRING, `city` STRING, `county` STRING, `postal` STRING, `phone1` STRING, `phone2` STRING, `email` STRING, `web` STRING)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `escape` '"',
  `header` 'true',
  `delimiter` ',',
  path 'dbfs:/FileStore/tables/uk_500-626ee.csv'
);

DROP TABLE IF EXISTS data_table_anon;

CREATE TABLE `data_table_anon` (`first_name` STRING, `last_name` STRING, `company_name` STRING, `address` STRING, `city` STRING, `county` STRING, `postal` STRING, `phone1` STRING, `phone2` STRING, `email` STRING, `web` STRING)
USING com.databricks.spark.csv
OPTIONS (
  `multiLine` 'false',
  `escape` '"',
  `header` 'true',
  `delimiter` ',',
  path 'dbfs:/FileStore/tables/uk_500-626ee-anonymized.csv'
);

-- COMMAND ----------

SELECT * from data_table ORDER BY web LIMIT 5;

-- COMMAND ----------

SELECT * from data_table_anon ORDER BY web LIMIT 5;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/FileStore/tables/uk_500-626ee-anonymized.csv")
-- MAGIC display(dbutils.fs.ls("dbfs:/FileStore/tables/"))

-- COMMAND ----------

DROP TABLE data_table

-- COMMAND ----------

DROP TABLE data_table_anon
