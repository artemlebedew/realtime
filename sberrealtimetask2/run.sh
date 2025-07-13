spark-submit \
    --conf "spark.executor.env.PYSPARK_PYTHON=/usr/bin/python3" \
    --conf "spark.pyspark.driver.python=/usr/bin/python3" \
    --conf spark.pyspark.python=python3 \
    task2.py | grep -v WARNING