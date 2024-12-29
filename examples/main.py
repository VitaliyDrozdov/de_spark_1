from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("PySpark PostgreSQL Connection")
    .config("spark.jars", "postgresql-42.2.23.jar")
    .getOrCreate()
)

url = "jdbc:postgresql://localhost:5432/mydatabase"
properties = {
    "user": "myuser",
    "password": "mypassword",
    "driver": "org.postgresql.Driver",
}

df = spark.read.jdbc(url=url, table="employees", properties=properties)

df.show()

spark.stop()
