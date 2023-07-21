# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pydeequ.analyzers import AnalysisRunner, AnalyzerContext, Completeness, Size, DataType
from pyspark.sql import DataFrame
from typing import Dict, List
from pyspark.sql.types import (
    StructType,
    StructField, 
    StringType,
    FloatType,
)
from pyspark.sql.functions import lit

from utils.read_format import read_csv, read_parquet
from rule_types import (
    Size,
    unique,
    complete, 
    contained_in, 
    is_non_negative
)

spark = SparkSession.builder.getOrCreate()

class DataQuality:

    def __init__(self,
        dataframe: DataFrame = None,
        path: str = None,
        extension: str = None
    ):

        if dataframe:
            df = dataframe
        elif path:
            if extension == "parquet":
                df = read_parquet(spark=spark, path=path)
            elif extension == "csv":
                df = read_csv(spark=spark, path=path)

        self.df = df

    def check_data(self,
        notebook_name: str,
        rules:List[Dict[str, str]]
        ) -> DataFrame:
        
        """
        
        Perform data validation.

        Args:
            - notebook_name: Name of the notebook where we apply the function.
            - rules: Rules to apply to the dataset.
                E.g: [
                    {
                        "code_rule": "size",
                        "condition_rule": "greater_than",
                        "value": 100,
                        "columns": "stock"
                    },
                    {
                        "code_rule": "contained_in",
                        "value": ["ios", "android"],
                        "columns": "platform"
                    }
                ]

        """
        
        if not self.df.rdd.isEmpty():

            schema_check = StructType([
                StructField("check", StringType(), True),
                StructField("check_level", StringType(), True),
                StructField("check_status", StringType(), True),
                StructField("constraint", StringType(), True),
                StructField("constraint_status", StringType(), True),
                StructField("constraint_message", StringType(), True),
                StructField("instance_name", StringType(), True),
                StructField("rule_name", StringType(), True)]
            )

            df_check = spark.createDataFrame([], schema_check)
            
            if rules:

                for rule in rules:
                    
                    code_rule = rule["code_rule"]
                    condition_rule = rule["condition_rule"]
                    value = rule["value"]
                    columns = list(rule["columns"])
                    
                    if code_rule == "size":

                        if condition_rule == "greater_than":

                            results = Size.greater_than(spark=spark,df=self.df, value=value)

                            df_check = df_check.union(results)

                        elif condition_rule == "less_than":

                            results = Size.less_than(spark=spark,df=self.df, value=value)

                            df_check = df_check.union(results)

                        elif condition_rule == "greater_equal_to":

                            results = Size.greater_equal_to(spark=spark,df=self.df, value=value)

                            df_check = df_check.union(results)

                        elif condition_rule == "less_equal_to":

                            results = Size.less_equal_to(spark=spark,df=self.df, value=value)

                            df_check = df_check.union(results)

                        elif condition_rule == "equal_to":

                            results = Size.equal_to(spark=spark,df=self.df, value=value)

                            df_check = df_check.union(results)
                            
                    if code_rule == "unique":

                        for column in columns:

                            results = unique(spark=spark,df=self.df, column=column)

                            df_check = df_check.union(results)

                    if code_rule == "complete":

                        for column in columns:

                            results = complete(spark=spark,df=self.df, column=column)

                            df_check = df_check.union(results)

                    if code_rule == "contained_in":

                        results = contained_in(spark=spark,df=self.df, column=column, value=value)

                        df_check = df_check.union(results)

                    if code_rule == "is_non_negative":

                        for column in columns:

                            results = is_non_negative(spark=spark,df=self.df, column=column)

                            df_check = df_check.union(results)

                df_check = df_check.withColumn("notebook_name",lit(notebook_name)) \
                        .withColumnRenamed("constraint_status", "rule_status") \
                        .withColumnRenamed("constraint_message", "rule_message") \
                        .select([
                            "notebook_name",
                            "rule_name",
                            "instance_name",
                            "rule_status",
                            "rule_message"
                            ]
                        )

                return  df_check
        else:
            raise Exception("Empty Dataframe")

    def analyze_data(self,
        notebook_name: str, 
        columns_name: List[str], 
        ) -> DataFrame:

        """
        Analyze and extract metrics from the data.

        Args:
            - notebook_name: Name of the notebook where we apply the function.
            - columns_name: List with columns to analyze their data type and completeness.

        """
        
        if not self.df.rdd.isEmpty():
            
            schema_analyze = StructType([
                StructField("entity", StringType(), True),
                StructField("instance", StringType(), True),
                StructField("name", StringType(), True),
                StructField("value", FloatType(), True),
                StructField("instance_name", StringType(), True)])
            
            df_analyze = spark.createDataFrame([], schema_analyze)

            size = AnalysisRunner(spark) \
                            .onData(self.df) \
                            .addAnalyzer(Size()) \
                            .run()

            size_check = AnalyzerContext.successMetricsAsDataFrame(spark, size)
            size_check = size_check.withColumn("instance_name", lit("dataframe"))

            df_analyze = df_analyze.union(size_check)
            

            for column in columns_name:

                analysis_data_type  = AnalysisRunner(spark) \
                                .onData(self.df) \
                                .addAnalyzer(DataType(column)) \
                                .run()

                analysis_df_type = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_data_type)
                analysis_df_type = analysis_df_type.withColumn("instance_name", lit(column))

                df_analyze = df_analyze.union(analysis_df_type)

                analysis_completeness  = AnalysisRunner(spark) \
                                .onData(self.df) \
                                .addAnalyzer(Completeness(column)) \
                                .run()

                analysis_df_completeness = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_completeness)
                analysis_df_completeness = analysis_df_completeness.withColumn("instance_name", lit(column))

                df_analyze = df_analyze.union(analysis_df_completeness)
            
            df_analyze = df_analyze.withColumn("notebook_name", lit(notebook_name)) \
                    .withColumnRenamed("name","analyze_name") \
                    .select([
                        "notebook_name",
                        "file_name",
                        "instance_name",
                        "analysis_name",
                        "value"]
                    )

            return df_analyze
        else:
            raise Exception("Empty Dataframe")