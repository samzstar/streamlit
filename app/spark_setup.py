import os

def setup_spark():
    # Setting the java home and spark home environment
    # os.environ["JAVA_HOME"] = "C:\Program Files\Java\jdk-21"
    # os.environ["SPARK_HOME"] = "C:\pyspark\spark-3.5.5"

    # Using findspark to locate spark
    import findspark
    findspark.init()
    
    # Creating a pyspark session
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("ClashData") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.catalog.clearCache()
    
    return spark
