from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.appName("My App").getOrCreate()
    schema = StructType(
        fields=[
            StructField("emp_id", StringType(), True),
            StructField("fname", StringType(), True),
            StructField("lname", StringType(), True),
            StructField("address", StringType(), True),
            StructField("city", StringType(), True),
            StructField("state", StringType(), True),
            StructField("zipcode", StringType(), True),
            StructField("job_title", StringType(), True),
            StructField("email", StringType(), True),
            StructField("active", StringType(), True),
            StructField("salary", IntegerType(), True),
        ])
    df = spark.read.csv("data/*", sep=r'\t', header=False, schema=schema).persist()
    df.createOrReplaceTempView("employees")

    print("==================EX1==================")
    # Show the first five employees ordered by last name and first name
    ex1_df = df.select("fname", "lname").orderBy(["lname", "fname"], ascending= [1, 1]).limit(5).collect()
    ex1_sql = df.sparkSession.sql("SELECT fname, lname FROM employees ORDER BY lname, fname LIMIT 5").collect()
    print(ex1_df)
    print(ex1_sql)

    print("==================EX2==================")
    # Count how many distinct job_titles there are
    ex2_df = df.select("job_title").distinct().count()
    # ex2_sql = df.sparkSession.sql("SELECT count(distinct(job_title)) FROM {df}", df=df).collect()
    ex2_sql = df.sparkSession.sql("SELECT distinct(job_title) FROM employees").count()
    print(ex2_df)
    print(ex2_sql)

    print("==================EX3==================")
    # For each job_title show the number of employees and the average salary
    ex3_df = df.select(["salary", "job_title"]).groupBy("job_title")\
                                                .agg({'salary': 'avg', '*': 'count'})\
                                                .collect()
    ex3_sql = df.sparkSession.sql("SELECT job_title, COUNT(*), AVG(salary) FROM employees GROUP BY job_title").collect()
    print(ex3_df)
    print(ex3_sql)