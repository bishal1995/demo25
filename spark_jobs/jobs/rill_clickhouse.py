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
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    
    # Collect and print word counts
    for word, count in wordCounts.collect():
        print(f"{word}: {count}")
    
    # Stop the Spark session
    spark.stop()

if __name__ == "__main__":
    main()