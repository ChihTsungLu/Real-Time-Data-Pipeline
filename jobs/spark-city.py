from pyspark.sql import SparkSession
from config import configuration
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
from pyspark.sql.functions import from_json, col
from pyspark.sql import DataFrame, SparkSession
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    spark = SparkSession.builder.appName("SparkCity")\
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk-s3:1.11.469,"
        )\
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configuration["AWS_ACCESS_KEY"])\
        .config("spark.hadoop.fs.s3a.secret.key", configuration["AWS_SECRET_KEY"])\
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
        .getOrCreate()

    try:
        spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", configuration["AWS_ACCESS_KEY"])
        spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", configuration["AWS_SECRET_KEY"])
        test_df = spark.read.text("s3a://spark-data-alvis/")
        logger.info("Successfully connected to S3")
    except Exception as e:
        logger.error(f"Failed to connect to S3: {str(e)}")
        raise
        
    spark.sparkContext.setLogLevel("WARN")

    #vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])
    
    #gpsSchema
    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])
    
    #trafficSchema
    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])
    
    #weatherSchema
    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])
    
    #emergencySchema
    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),        
    ])
    
    def read_kafka_topic(topic, schema, kafka_broker="broker:29092"):
        """
        Read a Kafka topic with enhanced error handling and logging
        
        :param topic: Kafka topic name
        :param schema: PySpark schema for the topic
        :param kafka_broker: Kafka broker address
        :return: Streaming DataFrame
        """
        try:
            logger.info(f"Attempting to read Kafka topic: {topic}")
            return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_broker)
                .option("subscribe", topic)
                .option("startingOffsets", "earliest")
                .load()
                .selectExpr('CAST(value AS STRING)')
                .select(from_json(col('value'), schema).alias('data'))
                .select('data.*')
                .withWatermark('timestamp', '1 minutes')
            )
        except Exception as e:
            logger.error(f"Error reading Kafka topic {topic}: {str(e)}")
            raise
    
    def streamWriter(input: DataFrame, checkpointFolder, output):
        """
        Write streaming data to S3 with checkpoint support
        
        :param input: Input streaming DataFrame
        :param checkpointFolder: S3 path for checkpoints
        :param output: S3 path for output data
        :return: Streaming query
        """
        return(input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    
    # Dynamically reading topics with error handling
    topics_schemas = [
        ('vehicle_data', vehicleSchema),
        ('gps_data', gpsSchema),
        ('traffic_data', trafficSchema),
        ('weather_data', weatherSchema),
        ('emergency_data', emergencySchema)
    ]
    
    streaming_queries = []
    
    for topic, schema in topics_schemas:
        try:
            # Read Kafka topic
            df = read_kafka_topic(topic, schema)
            
            # Create streaming query
            checkpoint_path = f's3a://spark-data-alvis/checkpoints/{topic}'
            output_path = f's3a://spark-data-alvis/data/{topic}'
            
            query = streamWriter(df, checkpoint_path, output_path)
            streaming_queries.append(query)
            
            logger.info(f"Created streaming query for topic: {topic}")
        
        except Exception as e:
            logger.error(f"Failed to process topic {topic}: {str(e)}")
    
    # Wait for the last query to terminate
    if streaming_queries:
        streaming_queries[-1].awaitTermination()
    else:
        logger.warning("No streaming queries were created.")
    
if __name__ == "__main__":
    main()