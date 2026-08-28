# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
try:
    DEST_FULL_TABLE_NAME
except:
    DEST_FULL_TABLE_NAME = "`Playground`.`test_lakehouse`.`dbo`.`test_table`"

df = spark.createDataFrame(
    [
        (1, "Jose", 100.50),
        (2, "Ana", 200.00),
        (3, "John", 50.25),
        (4, "Maria", 300.75),
    ],
    ["id", "name", "amount"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = (
    df
    .filter(sf.col("amount") > 100)
    .withColumn("amount_with_tax", sf.round(sf.col("amount") * 1.21, 2))
    .withColumn("name_upper", sf.upper("name"))
    .select("id", "name_upper", "amount", "amount_with_tax")
    .orderBy(sf.col("amount").desc())
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# This is now GITHUB

# CELL ********************

result.write.mode("overwrite").saveAsTable(DEST_FULL_TABLE_NAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
