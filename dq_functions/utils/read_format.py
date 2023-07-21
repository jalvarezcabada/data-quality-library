
def read_parquet(spark, path: str):
    df = spark.read.format('parquet') \
                    .option('header', True) \
                    .load(path)

    return df

def read_csv(spark, path: str, sep: str = ';'):
    df = spark.read.format('csv') \
            .option('sep',sep) \
            .option('header', True) \
            .load(path)

    return df