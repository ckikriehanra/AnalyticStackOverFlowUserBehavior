from pyspark.sql import SparkSession

from lib.utils import get_spark_app_config, load_questions_df, standardize_questions_df, test_question_standardized, \
    count_programing_language, count_domains, sum_score_per_day, sum_score_in_range_time, load_answers_df, \
    standardize_answers_df, find_good_questions, find_good_questions_detail_df, get_OwnerUserId_score, bucket_join, \
    find_active_user

START_DATE = "01-01-2008"
END_DATE = "01-01-2009"

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder \
        .config(conf=conf) \
        .getOrCreate()

    # Read data from collection names "Questions"
    questions_df = load_questions_df(spark).drop("_id")
    # questions_df.printSchema()

    # Standardize
    questions_standardized_df = standardize_questions_df(questions_df)
    # questions_standardized_df.printSchema()

    # Read data from collection "Answers"
    answers_df = load_answers_df(spark).drop("_id")
    answers_standardized_df = standardize_answers_df(answers_df)
    # answers_standardized_df.printSchema()

    # Check DF after Standardizing
    # test_question_standardized(questions_standardized_df)

    # Requirement 1: Count programing languages
    # questions_programing_language_df = count_programing_language(questions_standardized_df).show()

    # Requirement 2: Get 20 domain mostly used
    # questions_domain_df = count_domains(questions_standardized_df)

    # # Get dataframe only has OwnerUserId, Score from both questions_standardized_df, answers_standardized_df
    # ownerUserId_score_df = get_OwnerUserId_score(questions_standardized_df, answers_standardized_df)
    #
    # # Requirement 3: Get score of each user in each day
    # # score_users_per_day = sum_score_per_day(ownerUserId_score_df).show()
    #
    # # Requirement 4: Get score of each user in range of time
    # # score_users_in_range_time = sum_score_in_range_time(ownerUserId_score_df, START_DATE, END_DATE).show()

    # # Join two dataframe answers_standardized_df and questions_standardized_df
    # join_df = bucket_join(spark, questions_standardized_df, answers_standardized_df)
    # # join_df.printSchema()
    # # join_df.show(5)
    #
    # # Requirement 5: Find good questions with details
    # # good_questions_detail_df = find_good_questions_detail_df(join_df).show()
    #
    # # Requirement 6: Find active users
    # # active_users_df = find_active_user(join_df)
    # # active_users_df.show()

    # Find good questions. It is a simple way to do requirement 5
    # ParentID equals QuestionID
    # good_questions_df = find_good_questions(answers_standardized_df).show()


