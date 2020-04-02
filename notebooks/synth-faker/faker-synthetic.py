# Databricks notebook source
# MAGIC %sh pip install Faker

# COMMAND ----------

import csv
from faker import Faker
from faker.providers import *
import datetime

def datagenerate(records, headers):
    fake = Faker('en_GB')
    with open("/dbfs/FileStore/tables/People_data.csv", 'wt') as csvFile:
        writer = csv.DictWriter(csvFile, fieldnames=headers)
        writer.writeheader()
        for i in range(records):
            full_name = fake.name()
            FLname = full_name.split(" ")
            Fname = FLname[0]
            Lname = FLname[1]
            domain_name = "@testDomain.com"
            userId = Fname +"."+ Lname + domain_name
            
            writer.writerow({
                    "email" : userId,
                    "prefix" : fake.prefix(),
                    "name": fake.name(),
                    "dob" : fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2000, 1,1)),
                    "tele" : fake.phone_number(),
                    "email": fake.email(),
                    "address" : fake.address(),
                    "postcode" : fake.postcode(),
                    "city" : fake.city(),
                    "country" : fake.country(),
                    "year": fake.year(),
                    "time": fake.time(),
                    })
    
if __name__ == '__main__':
    records = 100000
    headers = ["email", "prefix", "name", "dob", "tele", "email",
               "address", "postcode", "city", "country", "year", "time"]
    datagenerate(records, headers)
    print("CSV generation complete!")

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP TABLE IF EXISTS synth_user_data;
# MAGIC 
# MAGIC CREATE TABLE `synth_user_data` (`email` STRING, `prefix` STRING, `name` STRING, `dob` STRING, `tele` STRING, `email.1` STRING, `address` STRING, `postcode` STRING, `city` STRING, `county` STRING, `year` STRING, `time` STRING)
# MAGIC USING com.databricks.spark.csv
# MAGIC OPTIONS (
# MAGIC   `multiLine` 'true',
# MAGIC   `escape` '"',
# MAGIC   `header` 'true',
# MAGIC   `delimiter` ',',
# MAGIC   path '/FileStore/tables/People_data.csv'
# MAGIC );

# COMMAND ----------

# MAGIC %sql SELECT * from default.synth_user_data order by name limit 10;

# COMMAND ----------

import csv
datafile = open('/dbfs/FileStore/tables/People_data.csv', 'r')
myreader = csv.reader(datafile)

for row in myreader:
    print(row)
