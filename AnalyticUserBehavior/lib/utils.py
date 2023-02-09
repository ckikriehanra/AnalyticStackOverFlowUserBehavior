import configparser
import re

from pyspark import SparkConf
from pyspark.sql import Window
from pyspark.sql.functions import *
from pyspark.sql.types import *
from lib.udfs import find_programing_language, get_url


def get_spark_app_config():
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for (key, val) in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf


def load_questions_df(spark):
    return spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/ASM1_data.Questions") \
        .load()


def load_answers_df(spark):
    return spark.read \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/ASM1_data.Answers") \
        .load()


def write_query(result_df):
    # Write data to folder output and make checkpoint
    # Trigger in mode processingTime with distance 1 minute
    return result_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="1 minute") \
        .start()


def standardize_questions_df(questions_df):
    return questions_df.withColumn("ClosedDate", to_date(regexp_extract(col("ClosedDate"), r"([0-9-]+)T", 1),
                                                         "yyyy-MM-dd")) \
        .withColumn("CreationDate", to_date(regexp_extract(col("CreationDate"), r"([0-9-]+)T", 1),
                                            "yyyy-MM-dd")) \
        .withColumn("OwnerUserId", col("OwnerUserId").cast(IntegerType()))


def standardize_answers_df(answers_df):
    return answers_df.withColumn("CreationDate", to_date(regexp_extract(col("CreationDate"), r"([0-9-]+)T", 1),
                                                         "yyyy-MM-dd")) \
        .withColumn("OwnerUserId", col("OwnerUserId").cast(IntegerType()))


def test_question_standardized(questions_standardized_df):
    questions_standardized_df.printSchema()
    questions_standardized_df.filter(col("OwnerUserId") == "NA").show()


def count_programing_language(questions_standardized_df):
    find_programing_languageUDF = udf(find_programing_language, ArrayType(StringType()))
    return questions_standardized_df.withColumn("Programing_languages", find_programing_languageUDF(col("Body"))) \
        .withColumn("Programing_languages", explode("Programing_languages")) \
        .groupBy("Programing_languages") \
        .count()


def count_domains(questions_standardized_df):
    get_urlUDF = udf(get_url, ArrayType(StringType()))
    return questions_standardized_df.withColumn("Url", get_urlUDF(col("Body"))) \
        .withColumn("Url", explode(col("Url"))) \
        .withColumn("Domain", regexp_extract(col("Url"), r"https*:\/\/([a-zA-Z0-9.]+)\/", 1)) \
        .groupBy("Domain") \
        .count() \
        .orderBy(col("count").desc()) \
        .filter(col("Domain") != "") \
        .select("Domain", "count") \
        .show(20, truncate=False)


def get_OwnerUserId_score(questions_standardized_df, answers_standardized_df):
    # Get OwnerUserId and Score from both questions_standardized_df and answers_standardized_df
    # Using unionAll to avoid lost duplicate data
    temp_df = questions_standardized_df.select("OwnerUserId", "CreationDate", "Score") \
        .filter(col("OwnerUserId").isNotNull())
    return answers_standardized_df.select("OwnerUserId", "CreationDate", "Score") \
        .filter(col("OwnerUserId").isNotNull()) \
        .unionAll(temp_df)


def sum_score_per_day(ownerUserId_score_df):
    total_score_window = Window \
        .partitionBy("OwnerUserId") \
        .orderBy("CreationDate") \
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    return ownerUserId_score_df.filter(col("OwnerUserId").isNotNull()) \
        .withColumn("TotalScore", sum("Score").over(total_score_window))


def restructure_date(date):
    re_pattern = r"[0-9]{2}-[0-9]{2}-[0-9]{4}"
    if re.match(re_pattern, date):
        parts = date.split("-")
        year = parts[2]
        month = parts[1]
        day = parts[0]
        return year + "-" + month + "-" + day
    else:
        return date


def sum_score_in_range_time(ownerUserId_score_df, start_date, end_date):
    # Restructure date: 01-01-2008 -> 2008-01-01
    start_date = restructure_date(start_date)
    end_date = restructure_date(end_date)

    return ownerUserId_score_df.filter((col("CreationDate") >= start_date) & (col("CreationDate") <= end_date)) \
        .groupBy("OwnerUserId") \
        .agg(sum("Score").alias("TotalScore")) \
        .orderBy("OwnerUserId")


def find_good_questions(answers_standardized_df):
    return answers_standardized_df.groupBy("ParentId") \
        .agg(count(col("*")).alias("No_answers")) \
        .filter(col("No_answers") > 5)


def bucketing_df_write_to_table(input_df, key, name_database, name_table):
    destination = name_database + "." + name_table
    input_df.coalesce(1).write \
        .bucketBy(3, key) \
        .mode("overwrite") \
        .saveAsTable(destination)


def bucket_join(spark, questions_standardized_df, answers_standardized_df):
    answers_standardized_df2 = answers_standardized_df.withColumnRenamed("Id", "AnswerId") \
        .withColumnRenamed("OwnerUserId", "answerUserId") \
        .withColumnRenamed("CreationDate", "CreationDateOfAnswer") \
        .withColumnRenamed("Score", "ScoreOfAnswer") \
        .withColumnRenamed("Body", "BodyOfAnswer")
    spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")

    # First, we need bucket two DF
    bucketing_df_write_to_table(questions_standardized_df, "Id", "MY_DB", "Questions_table")
    bucketing_df_write_to_table(answers_standardized_df2, "ParentId", "MY_DB", "Answers_table")

    # Second, reread two tables
    questions_tbl = spark.read.table("MY_DB.Questions_table")
    answers_tbl = spark.read.table("MY_DB.Answers_table")

    # Last, Join two tables
    join_expr = questions_tbl.Id == answers_tbl.ParentId

    join_df = questions_tbl.join(answers_tbl, join_expr, "inner")
    return join_df


def find_good_questions_detail_df(join_df):
    return join_df.groupBy("Id", "Title") \
        .agg(count(col("*")).alias("No_answers")) \
        .filter(col("No_answers") > 5) \
        .orderBy("Id")


def find_active_user(join_df):
    # Condition1: has more than 50 answers or sum of answer's score greater than 500
    df1 = join_df.groupBy("answerUserId") \
        .agg(count(col("*")).alias("no_answers"),
             sum(col("Score")).alias("total_answer_score")) \
        .filter((col("no_answers") > 50) | (col("total_answer_score") > 500)) \
        .select("answerUserId")

    # Condition2: has more than 5 answers in date when questions created
    df2 = join_df.filter(col("CreationDateOfAnswer") == col("CreationDate")) \
        .groupBy("answerUserId") \
        .agg(count("*").alias("no_answers_same_date_question")) \
        .filter(col("no_answers_same_date_question") > 5) \
        .select("answerUserId")

    # Because to avoid duplicate answerUserId then using union, not unionAll
    return df1.union(df2)

# def make_trade_df(value_df):
#     return value_df.select("value.*") \
#         .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
#         .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
#         .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))
#
#
# def make_window_agg(trade_df):
#     return trade_df \
#         .groupBy(  # col("BrokerCode"),
#         window(col("CreatedTime"), "15 minute")) \
#         .agg(sum("Buy").alias("TotalBuy"),
#              sum("Sell").alias("TotalSell"))
#
#
# def get_output_df(window_agg_df):
#     return window_agg_df.select(expr("window.start as start"), expr("window.end as end"), "TotalBuy", "TotalSell")
#
