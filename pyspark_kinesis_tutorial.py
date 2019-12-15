from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('<app_name>') \
    .config("spark.jars", "spark-sql-kinesis_2.11-2.4.0.jar") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming-kinesis-asl_2.11:2.4.4") \
    .enableHiveSupport() \
    .getOrCreate()


kinesisSpark = spark.readStream \
    .format('kinesis') \
    .option('streamName', '<aws_kinesis_stream_name>') \
    .option('region', '<your_region>') \
    .option('endpointUrl', 'https://kinesis.us-east-1.amazonaws.com/') \
    .option('startingPosition', 'earliest') \
    .option('awsAccessKeyId', '<your_access_key>') \
    .option('awsSecretKey', '<your_secret_key>') \
    .load()


userDF = kinesisSpark \
    .selectExpr('CAST (data as STRING)').alias("stream_data") \
    .writeStream \
    .format('console') \
    .option('truncate', 'false') \
    .start() \
    .awaitTermination()
