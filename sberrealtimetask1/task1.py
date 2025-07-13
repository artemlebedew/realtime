from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

spark = SparkSession.builder.appName('Collocations').master('yarn').getOrCreate()
articles_input_data = "/data/wiki/en_articles_part"
stop_words_input_data = "/data/wiki/stop_words_en-xpo6.txt"

stop_words_schema = StructType([
    StructField("stop_word", StringType())
])
stop_words = spark.read.format("csv") \
                  .schema(stop_words_schema) \
                  .load(stop_words_input_data)

words_schema = StructType([
    StructField("article_id", IntegerType()),
    StructField("article_text", StringType())
])
words = spark.read.format("csv") \
             .schema(words_schema) \
             .option("sep", "\t") \
             .load(articles_input_data) \
             .select(explode(split(lower(col("article_text")), " ")).alias("word")) \
             .select(regexp_replace(trim(col("word")), "^\W+|\W+$", "").alias("word")) \
             .filter(col("word").isNotNull() & (trim(col("word")) != "")) \
             .join(broadcast(stop_words), col("word") == stop_words["stop_word"], "left_anti")

words_count = words.count()

bigrams = words.withColumn("word_b", lead("word", 1).over(Window.partitionBy(spark_partition_id()).orderBy(monotonically_increasing_id()))) \
               .filter(col("word_b").isNotNull()) \
               .withColumn("bigram", concat_ws("_", col("word"), col("word_b"))) \
               .withColumnRenamed("word", "word_a") \
               .groupBy("word_a", "word_b", "bigram").agg(count("*").alias("count")) \
               .withColumn("p_ab", col("count") / (words_count - 1)) \
               .filter(col("count") >= 500) \
               .drop("count")

words_probability = words.groupBy("word").agg(count("*").alias("count")) \
                         .withColumn("probability", col("count") / words_count) \
                         .drop("count") \
                         .cache()

result = bigrams.join(words_probability, bigrams["word_a"] == words_probability["word"], "left") \
                .drop("word", "word_a") \
                .withColumnRenamed("probability", "p_a") \
                .join(words_probability, bigrams["word_b"] == words_probability["word"], "left") \
                .drop("word", "word_b") \
                .withColumnRenamed("probability", "p_b") \
                .withColumn("pmi", log(col("p_ab") / (col("p_a") * col("p_b")))) \
                .withColumn("npmi", -col("pmi") / log(col("p_ab"))) \
                .orderBy(desc("npmi")) \
                .select(col("bigram")) \
                .limit(39) \
                .coalesce(1) \
                .cache()

for i in result.take(39):
    print(i.bigram)
    
spark.stop()