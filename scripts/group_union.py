from pyspark.sql import SparkSession, functions as f

def ler_dataframe(spark):

    df_cars = spark.read.json(r'C:\Users\rafae\Projetos\git\pyspark_cursos\dataframes\car.json')
    # df_cars.show()
    # df_cars.groupBy('brand').count().show()
    df_cars.groupBy('brand').agg(
        f.count('Horsepower').alias('count'),
        f.max('Horsepower'),
        f.min('Horsepower'),
        f.avg('Horsepower').alias('Media')

    ).orderBy('count').show()


def unir_dataframe(spark):

    df_cars = spark.read.json(r'C:\Users\rafae\Projetos\git\pyspark_cursos\dataframes\car.json')
    df_audi = df_cars.filter('brand = "audi"')
    df_bmw = df_cars.filter('brand = "bmw"')
    df_audi.union(df_bmw).show()

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()

    # ler_dataframe(spark=spark)
    unir_dataframe(spark=spark)

    spark.stop()
