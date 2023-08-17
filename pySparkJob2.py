import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, count

sc = pyspark.SparkContext()
sqlContext = pyspark.sql.SQLContext(sc)

inputdir = sys.argv[1]

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(inputdir+'transaction.csv')

# Average amount
average_amount = df.select(avg("amt")).first()[0]

# Step 4: Group by merchant and calculate transaction_count and total_amount
filtered_df = df.groupBy("merchant").agg(
    sum("amt").alias("total_amount"),
    count("amt").alias("transaction_count")
)

# Total_amount > (average_amount * 2)
filtered_df = filtered_df.filter(filtered_df["total_amount"] > (average_amount * 2))

filtered_df = filtered_df.orderBy(col("total_amount").desc())

# Display the final result
filtered_df.show()
