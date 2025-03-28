from pyspark.sql import SparkSession

def main():
    # Create SparkSession
    spark = SparkSession.builder \
        .appName("PythonWordCount") \
        .getOrCreate()
    
    # Create text data
    text = "Hello Spark Hello Python Hello Airflow Hello Docker and Hello Yusuf"
    
    # Create RDD and perform word count
    words = spark.sparkContext.parallelize(text.split(" "))    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()