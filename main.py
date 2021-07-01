

import pyspark


spark = pyspark.sql.SparkSession \
    .builder \
    .appName("Airflow") \
    .config('spark.driver.extraClassPath', "/home/deepika/Documents/driver_jar_file") \
    .getOrCreate()


def load_to_db(df):
    mode="overwrite"
    url="jdbc:postgresql://localhost:5432/databasename"
    properties={"user":<username>,
                "password":<password>,
                "driver":"org.postgresql.Driver"
        
    }
    df.write.jdbc(url=url,
                  table="Project",
                  mode=mode,
                 properties= properties)

