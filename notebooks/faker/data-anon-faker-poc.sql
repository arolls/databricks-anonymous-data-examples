-- Databricks notebook source
-- MAGIC %md
-- MAGIC # data-anon-faker-poc

-- COMMAND ----------

DROP TABLE IF EXISTS datatable;

CREATE TABLE datatable
USING csv
OPTIONS (path "/FileStore/tables/uk_500-626ee.csv", header "true")

-- COMMAND ----------

SELECT * from datatable LIMIT 5

-- COMMAND ----------

-- MAGIC %sh pip install Faker unicodecsv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import csv
-- MAGIC from faker import Factory
-- MAGIC from collections import defaultdict
-- MAGIC 
-- MAGIC def anonymize_rows(rows):
-- MAGIC     """
-- MAGIC     Rows is an iterable of dictionaries that contain name and
-- MAGIC     email fields that need to be anonymized.
-- MAGIC     """
-- MAGIC     # Load faker
-- MAGIC     faker  = Factory.create()
-- MAGIC 
-- MAGIC     # Create mappings of rows to faked data.
-- MAGIC     names = defaultdict(faker.first_name)
-- MAGIC 
-- MAGIC     # Iterate over the rows from the file and yield anonymized rows.
-- MAGIC     for row in rows:
-- MAGIC         # Replace first_name with faked fields.
-- MAGIC         row["first_name"] = names[row["first_name"]]
-- MAGIC 
-- MAGIC         # Yield the row back to the caller
-- MAGIC         yield row
-- MAGIC 
-- MAGIC def anonymize(source, target):
-- MAGIC     """
-- MAGIC     The source argument is a path to a CSV file containing data to anonymize,
-- MAGIC     while target is a path to write the anonymized CSV data to.
-- MAGIC     """
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

DROP TABLE IF EXISTS dataanontable;

CREATE TABLE dataanontable
USING csv
OPTIONS (path "/FileStore/tables/uk_500-626ee-anonymized.csv", header "true")

-- COMMAND ----------

SELECT * from dataanontable LIMIT 5

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("dbfs:/FileStore/tables/uk_500-626ee-anonymized.csv")
-- MAGIC display(dbutils.fs.ls("dbfs:/FileStore/tables/"))
