import pyspark
import sys
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lag, udf
from pyspark.sql.types import StringType

sc = pyspark.SparkContext()
sqlContext = pyspark.sql.SQLContext(sc)

inputdir = sys.argv[1]

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(inputdir+'transaction.csv')

windowSpec = Window.partitionBy("cc_num").orderBy("trans_date_trans_time")

df = df.withColumn("num_transactions", F.count("trans_num").over(windowSpec))

df = df.withColumn("avg_transaction_amount", F.avg("amt").over(windowSpec))

df = df.withColumn("std_dev_transaction_amount", F.stddev("amt").over(windowSpec))

df = df.withColumn("prev_transaction_amount", F.lag("amt", 1).over(windowSpec))

def detect_fraud(amt, avg_transaction_amount, std_dev_transaction_amount, prev_transaction_amount, num_transactions):
    if amt is None or avg_transaction_amount is None or std_dev_transaction_amount is None or prev_transaction_amount is None:
        return 'None'
    
    if amt > (avg_transaction_amount + (3 * std_dev_transaction_amount)):
        return 'High Amount'
    elif amt > (1.5 * prev_transaction_amount):
        return 'Significant Increase'
    elif num_transactions > 10:
        return 'High Frequency'
    else:
        return 'None'

detect_fraud_udf = udf(detect_fraud, StringType())
df = df.withColumn("fraud_type", detect_fraud_udf("amt", "avg_transaction_amount", "std_dev_transaction_amount", "prev_transaction_amount", "num_transactions"))

potential_fraud_df = df.filter(df['fraud_type'] != 'None')

selected_columns = ['trans_num', 'amt', 'cc_num', 'trans_date_trans_time', 'fraud_type']
selected_potential_fraud_df = potential_fraud_df.select(selected_columns)

selected_potential_fraud_df.show()