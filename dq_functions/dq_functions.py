# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
import pymsteams
import os
from pydeequ.analyzers import *
from pydeequ.checks import *
from pydeequ.verification import *
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType,
    StructField, 
    StringType,
    FloatType,
    IntegerType
)
from pyspark.sql.functions import (
    col, 
    current_timestamp, 
    lit, 
    expr, 
    date_format
)

spark = SparkSession.builder.getOrCreate()

class DataQuality:

    @staticmethod
    def _check_qa(
        nombreNotebook:str,
        rules:list[dict], 
        path:str=None, 
        nombreArchivo:str=None, 
        extension:str=None, 
        dataframe:DataFrame=None,
        nombreDataframe:str=None, 
        save:bool=False
        ) -> DataFrame:
        
        """
        Funcion para realizar validacion sobre dataframes o archivos parquet/csv.

        Parametros:

            Obligatorios:

                - nombreNotebook: Nombre de la notebook en la que aplicamos la funcion.
                - rules: Reglas a aplicar al set de datos en formato list(dict).
                
            No obligatorios:

                - path: Ruta del archivo a validar en nuestro Datalake.
                - nombreArchivo: Nombre del archivo en Datalake.
                - extension: Tipo de extension parquet o csv.
                - dataframe: Datos a validar en formato DataFrame de Spark.
                - nombreDataframe: Nombre de la variable que contiene nuestro DataFrame.
                - save: Si se guardan o los los resultados en nuestra DB.

        """

        if dataframe:

            df = dataframe

        elif path:

            if extension == 'parquet':

                df = spark.read.format('parquet') \
                                .option('header', True) \
                                .load(path)

            elif extension == 'csv':

                df = spark.read.format('csv') \
                        .option('sep',';') \
                        .option('header', True) \
                        .load(path)
        
        if not df.rdd.isEmpty():

            schema_check = StructType([
                StructField("check", StringType(), True),
                StructField("check_level", StringType(), True),
                StructField("check_status", StringType(), True),
                StructField("constraint", StringType(), True),
                StructField("constraint_status", StringType(), True),
                StructField("constraint_message", StringType(), True),
                StructField("nombreInstancia", StringType(), True),
                StructField("nombreRegla", StringType(), True)]
            )

            df_final = spark.createDataFrame([], schema_check)
            
            if rules:

                for rule in rules:
                    
                    code_rule = rule['code_rule']
                    condition_rule = rule['condition_rule']
                    values = rule['values']
                    columns = list(rule['columns'])
                    
                    if code_rule == 'size':

                        if condition_rule == 'greater than':

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .hasSize(lambda x: x > int(values[0]))) \
                                .run()

                            checkResult_size = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_size = checkResult_size.withColumn('nombreInstancia', lit('dataframe')) \
                                                                .withColumn("nombreRegla", lit(f'{code_rule}: {condition_rule}'))

                            df_final = df_final.union(checkResult_size)

                        elif condition_rule == 'less than':

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .hasSize(lambda x: x < int(values[0]))) \
                                .run()

                            checkResult_size = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_size = checkResult_size.withColumn('nombreInstancia', lit('dataframe')) \
                                                                .withColumn("nombreRegla", lit(f'{code_rule}: {condition_rule}'))

                            df_final = df_final.union(checkResult_size)

                        elif condition_rule == 'greater equal to':

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .hasSize(lambda x: x >= int(values[0]))) \
                                .run()

                            checkResult_size = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_size = checkResult_size.withColumn('nombreInstancia', lit('dataframe')) \
                                                                .withColumn("nombreRegla", lit(f'{code_rule}: {condition_rule}'))

                            df_final = df_final.union(checkResult_size)

                        elif condition_rule == 'less equal to':

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .hasSize(lambda x: x <= int(values[0]))) \
                                .run()

                            checkResult_size = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_size = checkResult_size.withColumn('nombreInstancia', lit('dataframe')) \
                                                                .withColumn("nombreRegla", lit(f'{code_rule}: {condition_rule}'))

                            df_final = df_final.union(checkResult_size)

                        elif condition_rule == 'equal to':

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .hasSize(lambda x: x == int(values[0]))) \
                                .run()

                            checkResult_size = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_size = checkResult_size.withColumn('nombreInstancia', lit('dataframe')) \
                                                                .withColumn("nombreRegla", lit(f'{code_rule}: {condition_rule}'))

                            df_final = df_final.union(checkResult_size)
                            
                    if code_rule == 'unique':

                        for c in columns:

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck( Check(spark, CheckLevel.Error, "Check") \
                                .isUnique(c)) \
                                .run()

                            checkResult_unique = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_unique = checkResult_unique.withColumn('nombreInstancia', lit(c)) \
                                                                    .withColumn('nombreRegla',lit(code_rule))

                            df_final = df_final.union(checkResult_unique)

                    if code_rule == 'complete':

                        for c in columns:

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .isComplete(c)) \
                                .run()

                            checkResult_complete = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_complete = checkResult_complete.withColumn('nombreInstancia', lit(c)) \
                                                                        .withColumn('nombreRegla',lit(code_rule))

                            df_final = df_final.union(checkResult_complete)

                    if code_rule == 'contained_in':

                        checkResult_df = VerificationSuite(spark) \
                        .onData(df) \
                        .addCheck(Check(spark, CheckLevel.Error, "Check")\ 
                        .isContainedIn(str(columns[0]), values)) \
                        .run()

                        checkResult_contained = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                        checkResult_contained = checkResult_contained.withColumn('nombreInstancia', lit(str(columns[0]))) \
                                                                        .withColumn('nombreRegla',lit(f'contained {values} in'))

                        df_final = df_final.union(checkResult_contained)

                    if code_rule == 'is_non_negative':

                        for c in columns:

                            checkResult_df = VerificationSuite(spark) \
                                .onData(df) \
                                .addCheck(Check(spark, CheckLevel.Error, "Check") \
                                .isNonNegative(c)) \
                                .run()

                            checkResult_is_non_negative = VerificationResult.checkResultsAsDataFrame(spark, checkResult_df)
                            checkResult_is_non_negative = checkResult_is_non_negative.withColumn('nombreInstancia', lit(c)) \
                                                                                        .withColumn('nombreRegla',lit(code_rule))

                            df_final = df_final.union(checkResult_is_non_negative)

                df_final = df_final.withColumn('nombreNotebook',lit(nombreNotebook)) \
                                .withColumnRenamed('constraint_status', 'estadoRegla') \
                                .withColumnRenamed('constraint_message', 'mensajeRegla') \

                if dataframe:

                    df_final = df_final.withColumn('nombreDataframe', lit(nombreDataframe)) \
                                    .withColumn('nombreArchivo', lit('null'))
                elif path:

                    df_final = df_final.withColumn('nombreDataframe', lit('null')) \
                                        .withColumn('nombreArchivo', lit(nombreArchivo))

                df_final = df_final.select([
                                            'nombreNotebook',
                                            'nombreDataframe',
                                            'nombreArchivo',
                                            'nombreRegla',
                                            'nombreInstancia',
                                            'estadoRegla',
                                            'mensajeRegla'
                                            ]
                )
                    
                if save:

                    jdbcHostname = os.environ.get('DF_BD_HOST')
                    jdbcDatabase = os.environ.get('DF_BD_NAME')
                    username = os.environ.get('DF_BD_USER')
                    password = os.environ.get('DF_BD_PASS')

                    connectionProperties = {
                        "user" : username,
                        "password" : password,
                        "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                    }

                    jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:1433;database={jdbcDatabase};user={username};password={password}"

                    df_final.write.jdbc(url=jdbcUrl,
                                        table="dbo.controlDQChequeo_Deequ",
                                        mode="append", 
                                        properties=connectionProperties
                    )

                return  df_final

        else:

            raise Exception("Dataframe vacio")

    @staticmethod 
    def _alert_check(
        nombreNotebook:str,
        dataframe:DataFrame,
        campoCritico:str,
        datosOrigen:str=None,
        tablaDestino:str=None
        ) -> None:

        """
        Funcion para alertar a teams en caso de que algun campo critico no supere el chequeo.

        Parametros:

            Obligatorios:

                - nombreNotebook: Nombre de la notebook en la que aplicamos la funcion.
                - dataframe: DataFrame con los resultados restornados por la funcion _check_qa().
                - campoCritico: Campo que no deberia poseer errores en sus datos.

            No obligatorios:

                - datosOrigen: Nombre de la vista con los datos en proceso de validacion.
                - tablaDestino: Tabla de destino de los datos en proceso de validacion.

        """

        if dataframe != "Dataframe vacio":

            new_df = dataframe.where("estadoRegla == 'Failure'")

            if new_df.count() > 0:

                lista_campos = [row.nombreInstancia for row in new_df.select("nombreInstancia").collect()]

                if campoCritico not in lista_campos:

                    if datosOrigen and tablaDestino:
                    
                        query = f"""
                            INSERT INTO {tablaDestino}
                            SELECT * 
                            FROM {datosOrigen}
                        """
                        spark.sql(query)
                        spark.sql(f'OPTIMIZE {tablaDestino}')
                
                else:

                    myTeamsMessage = pymsteams.connectorcard("<<url_teams>>")
                    myTeamsMessage.title(f"QA - Notebook: {nombreNotebook} - Dataframe: {datosOrigen}")
                    myTeamsMessage.text(f"Error sobre campo: {campoCritico}")
                    myTeamsMessage.send()

            else:

                f"No se encontro error sobre campo: {campoCritico}"
        else:

            raise Exception("El Dataframe del chequeo esta vacio")

    @staticmethod
    def _analysis_qa(
        nombreNotebook:str, 
        path:str=None, 
        nombreArchivo:str=None, 
        extension:str=None, 
        dataframe:DataFrame=None, 
        nombreDataframe:str=None, 
        nombreColumna:list[str]=None, 
        save:bool=False
        ) -> DataFrame:

        """
        Funcion para analizar y sacar metricas sobre los datos provenientes de un Dataframe o archivo parquet/csv.

        Parametros:

            Obligatorios:

                - nombreNotebook: Nombre de la notebook en la que aplicamos la funcion.

            No obligatorios:

                - path: Ruta del archivo a validar en nuestro Datalake.
                - nombreArchivo: Nombre del archivo en Datalake.
                - extension: Tipo de extension parquet o csv.
                - dataframe: Datos a validar en formato DataFrame de Spark.
                - nombreDataframe: Nombre de la variable que contiene nuestro DataFrame.
                - nombreColumna: Lista con columnas a analizar su tipo de dato y completitud.
                - save: Si se guardan o los los resultados en nuestra DB.
        """
        

        if dataframe:

            df = dataframe

        elif path:

            if extension == 'parquet':

                df = spark.read.format('parquet') \
                                .option('header', True) \
                                .load(path)

            elif extension == 'csv':

                df = spark.read.format('csv') \
                        .option('sep',';') \
                        .option('header', True) \
                        .load(path)
        
        if not df.rdd.isEmpty():
            
            schema_analisis = StructType([
                StructField("entity", StringType(), True),
                StructField("instance", StringType(), True),
                StructField("name", StringType(), True),
                StructField("value", FloatType(), True),
                StructField("nombreInstancia", StringType(), True)])
            
            df_final = spark.createDataFrame([], schema_analisis)

            size = AnalysisRunner(spark) \
                            .onData(df) \
                            .addAnalyzer(Size()) \
                            .run()

            size_check = AnalyzerContext.successMetricsAsDataFrame(spark, size)
            size_check = size_check.withColumn('nombreInstancia', lit('Dataframe'))

            df_final = df_final.union(size_check)
            

            if nombreColumna:

                for column in nombreColumna:

                    analysis_data_type  = AnalysisRunner(spark) \
                                    .onData(df) \
                                    .addAnalyzer(DataType(column)) \
                                    .run()

                    analysis_df_type = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_data_type)
                    analysis_df_type = analysis_df_type.withColumn('nombreInstancia', lit(column))

                    df_final = df_final.union(analysis_df_type)

                    analysis_completeness  = AnalysisRunner(spark) \
                                    .onData(df) \
                                    .addAnalyzer(Completeness(column)) \
                                    .run()

                    analysis_df_completeness = AnalyzerContext.successMetricsAsDataFrame(spark, analysis_completeness)
                    analysis_df_completeness = analysis_df_completeness.withColumn('nombreInstancia', lit(column))

                    df_final = df_final.union(analysis_df_completeness)
            
            df_final = df_final.withColumn('nombreNotebook', lit(nombreNotebook)) \
                                .withColumnRenamed("name","nombreAnalisis") \
                                .withColumnRenamed("value","numeroResultado")

            if dataframe:

                df_final = df_final.withColumn('nombreDataframe', lit(nombreDataframe)) \
                                    .withColumn('nombreArchivo', lit('null'))
            elif path:

                df_final = df_final.withColumn('nombreDataframe', lit('null')) \
                                    .withColumn('nombreArchivo', lit(nombreArchivo))

            
            df_final = df_final.select(['nombreNotebook',
                                        'nombreDataframe',
                                        'nombreArchivo',
                                        'nombreInstancia',
                                        'nombreAnalisis',
                                        'numeroResultado']
            )
            
            if save:
                
                jdbcHostname = os.environ.get('DF_BD_HOST')
                jdbcDatabase = os.environ.get('DF_BD_NAME')
                username = os.environ.get('DF_BD_USER')
                password = os.environ.get('DF_BD_PASS')

                connectionProperties = {
                    "user" : username,
                    "password" : password,
                    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver"
                }

                jdbcPort = 1433
                jdbcUrl = f"jdbc:sqlserver://{jdbcHostname}:{jdbcPort};database={jdbcDatabase};user={username};password={password}"

                df_final.write.jdbc(url=jdbcUrl, table="dbo.controlDQAnalisis_Deequ", mode="append", properties=connectionProperties)

            return df_final

        else:

            raise Exception("Dataframe vacio")