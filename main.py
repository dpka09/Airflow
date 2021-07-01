

import pyspark


spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Airflow") \
    .config('spark.driver.extraClassPath', "/home/deepika/Documents/postgresql-42.2.22.jar") \
    .getOrCreate()


def load_to_db(df):
    mode="overwrite"
    url="jdbc:postgresql://localhost:5432/mytest"
    properties={"user":"postgres",
                "password":"qw3rty123",
                "driver":"org.postgresql.Driver"
        
    }
    df.write.jdbc(url=url,
                  table="Project",
                  mode=mode,
                 properties= properties)

