from pydeequ.analyzers import *
from pydeequ.checks import CheckLevel, Check
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql.functions import lit
from pyspark.sql import DataFrame

def verification_result(spark, df:DataFrame, instance_name: str, rule_name: str ):

    check_result_df = VerificationResult.checkResultsAsDataFrame(spark, df)
    check_result_df = check_result_df.withColumn("instance_name", lit(instance_name)) \
                                        .withColumn("rule_name", lit(rule_name))

    return check_result_df

def unique(spark, df:DataFrame, column: str):

    check_result_df = VerificationSuite(spark) \
        .onData(df) \
        .addCheck( Check(spark, CheckLevel.Error, "Check") \
        .isUnique(column)) \
        .run()

    check_result_unique_df = verification_result(spark = spark, df = check_result_df, instance_name=column, rule_name="unique")

    return check_result_unique_df

def complete(spark, df:DataFrame, column: str):

    check_result_df = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(Check(spark, CheckLevel.Error, "Check") \
        .isComplete(column)) \
        .run()

    check_result_complete_df = verification_result(spark = spark, df = check_result_df, instance_name=column, rule_name="complete")

    return check_result_complete_df

def contained_in(spark, df:DataFrame, column: str, value):

    check_result_df = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(Check(spark, CheckLevel.Error, "Check") \
        .isContainedIn(str(column), value)) \
        .run()

    check_result_contained_df = verification_result(spark = spark, df = check_result_df, instance_name=column, rule_name=f"contained {value} in")

    return check_result_contained_df

def is_non_negative(spark, df:DataFrame, column: str):

    check_result_df = VerificationSuite(spark) \
        .onData(df) \
        .addCheck(Check(spark, CheckLevel.Error, "Check") \
        .isNonNegative(column)) \
        .run()

    check_result_is_non_negative_df = verification_result(spark = spark, df = check_result_df, instance_name=column, rule_name="is_non_negative")

    return check_result_is_non_negative_df

class Size:

    @staticmethod
    def greater_than(spark, df:DataFrame, value):
        check_result_df = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(Check(spark, CheckLevel.Error, "Check") \
            .hasSize(lambda x: x > int(value))) \
            .run()

        check_result_size_df = verification_result(spark = spark, df = check_result_df, instance_name="dataframe", rule_name="greater_than_size")

        return check_result_size_df

    @staticmethod
    def less_than(spark, df:DataFrame, value):

        check_result_df = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(Check(spark, CheckLevel.Error, "Check") \
            .hasSize(lambda x: x < int(value))) \
            .run()

        check_result_size_df = verification_result(spark = spark, df = check_result_df, instance_name="dataframe", rule_name="less_than_size")

        return check_result_size_df

    @staticmethod
    def greater_equal_to(spark, df:DataFrame, value):

        check_result_df = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(Check(spark, CheckLevel.Error, "Check") \
            .hasSize(lambda x: x >= int(value))) \
            .run()

        check_result_size_df = verification_result(spark = spark, df = check_result_df, instance_name="dataframe", rule_name="greater_equal_to_size")

        return check_result_size_df

    @staticmethod
    def less_equal_to(spark, df:DataFrame, value):

        check_result_df = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(Check(spark, CheckLevel.Error, "Check") \
            .hasSize(lambda x: x <= int(value))) \
            .run()

        check_result_size_df = verification_result(spark = spark, df = check_result_df, instance_name="dataframe", rule_name="less_equal_to_size")

        return check_result_size_df

    @staticmethod
    def equal_to(spark, df:DataFrame, value):

        check_result_df = VerificationSuite(spark) \
            .onData(df) \
            .addCheck(Check(spark, CheckLevel.Error, "Check") \
            .hasSize(lambda x: x == int(value))) \
            .run()

        check_result_size_df = verification_result(spark = spark, df = check_result_df, instance_name="dataframe", rule_name="equal_to_size")

        return check_result_size_df
