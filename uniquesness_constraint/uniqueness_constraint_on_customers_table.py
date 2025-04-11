from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count


spark = SparkSession.builder.appName("SplitUniqueAndDuplicateCustomers").getOrCreate()


df = spark.read.option("multiline", "true").json(r"C:\Users\HP\PycharmProjects\ecom-medallion\data\dq_rules_sample_datasets\03_Uniqueness_Constraint\r2_customer_id_in_Customers_Table_should_not_have_duplicate")


duplicate_ids_df = (
    df.groupBy("customer_id")
    .count()
    .filter(col("count") > 1)
    .select("customer_id")
)


duplicate_records_df = df.join(duplicate_ids_df, on="customer_id", how="inner")


valid_records_df = df.join(duplicate_ids_df, on="customer_id", how="left_anti")


valid_records_df.write.mode("overwrite").json(r"C:\Users\HP\PycharmProjects\ecom-resolve\unique_constraints\unique_customer_id")
duplicate_records_df.write.mode("overwrite").json(r"C:\Users\HP\PycharmProjects\ecom-resolve\unique_constraints\duplicate_constraints")
