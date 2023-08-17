import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, datediff, when

#SparkContext
sc = pyspark.SparkContext()
sqlContext = pyspark.sql.SQLContext(sc)

inputdir = sys.argv[1]

# Load the data into a DataFrame
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(inputdir+'transaction.csv')

# Step 3: Calculate the age of each cardholder and categorize them into age groups
df = df.withColumn("dob", df["dob"].cast("date"))
df = df.withColumn("age", datediff(current_date(), df["dob"]) / 365)
df = df.withColumn(
    "AgeGroup",
    when(col("age") < 18, "Under 18")
    .when((col("age") >= 18) & (col("age") <= 30), "18-30")
    .when((col("age") >= 31) & (col("age") <= 50), "31-50")
    .otherwise("Over 50")
)

# Step 4: Group by AgeGroup and calculate total transactions
result_df = df.groupBy("AgeGroup").agg({"age": "count"}).withColumnRenamed("count(age)", "TotalTransactions")

# Display the final result
print("AgeGroup with total transactions : ")
result_df.show()
