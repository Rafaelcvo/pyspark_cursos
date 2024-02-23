from pyspark.sql import SparkSession



if __name__ == '__main__':

    spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    data = [[1, 'user1'], [2, 'user2'], [3, 'user3']]
    columns = ['id', 'name']

    df = spark.createDataFrame(schema=columns, data=data)
    # df.show()
    # df.printSchema()
    print(df.count())

    spark.stop()