import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, struct, to_json, to_timestamp
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


JOB_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("location", StringType(), True),
    StructField("published", StringType(), True),
    StructField("description", StringType(), True),
    StructField("application_url", StringType(), True),
    StructField("has_remote", BooleanType(), True),
    StructField("salary_min", DoubleType(), True),
    StructField("salary_max", DoubleType(), True),
    StructField("salary_currency", StringType(), True),
    StructField("company", StructType([
        StructField("name", StringType(), True),
    ]), True),
])

spark_session = None


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} must be set")
    return value


def s3_path(bucket, *parts):
    key = "/".join(part.strip("/") for part in parts if part)
    return f"s3a://{bucket}/{key}"


def optional_env(name, default):
    return os.getenv(name) or default


def configure_s3(builder):
    region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
    builder = (
        builder
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint.region", region)
    )

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    session_token = os.getenv("AWS_SESSION_TOKEN")
    if access_key and secret_key:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.access.key", access_key)
            .config("spark.hadoop.fs.s3a.secret.key", secret_key)
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )
        if session_token:
            builder = (
                builder
                .config("spark.hadoop.fs.s3a.session.token", session_token)
                .config(
                    "spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
                )
            )

    endpoint = os.getenv("S3_ENDPOINT_URL")
    if endpoint:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", os.getenv("S3_PATH_STYLE_ACCESS", "true"))
        )

    return builder


def write_gold(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    gold_df = batch_df.select(
        col("id").cast("string").alias("id"),
        col("title").alias("job_title"),
        col("location").alias("location_name"),
        to_timestamp(col("published")).alias("published"),
        col("description").alias("job_description"),
        col("application_url"),
        col("company.name").alias("company_name"),
        col("salary_min"),
        col("salary_max"),
        col("salary_currency"),
    ).dropDuplicates(["id"])

    gold_df.write.jdbc(
        url=os.environ["POSTGRES_JDBC_URL"],
        table="jobs_gold_stage",
        mode="overwrite",
        properties={
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PASSWORD"],
            "driver": "org.postgresql.Driver",
        },
    )

    connection = spark_session._jvm.java.sql.DriverManager.getConnection(
        os.environ["POSTGRES_JDBC_URL"],
        os.environ["POSTGRES_USER"],
        os.environ["POSTGRES_PASSWORD"],
    )
    try:
        statement = connection.createStatement()
        statement.executeUpdate("""
            INSERT INTO jobs_gold (
                id, job_title, location_name, published, job_description,
                application_url, company_name, salary_min, salary_max, salary_currency
            )
            SELECT
                id, job_title, location_name, published, job_description,
                application_url, company_name, salary_min, salary_max, salary_currency
            FROM jobs_gold_stage
            ON CONFLICT (id) DO UPDATE SET
                job_title = EXCLUDED.job_title,
                location_name = EXCLUDED.location_name,
                published = EXCLUDED.published,
                job_description = EXCLUDED.job_description,
                application_url = EXCLUDED.application_url,
                company_name = EXCLUDED.company_name,
                salary_min = EXCLUDED.salary_min,
                salary_max = EXCLUDED.salary_max,
                salary_currency = EXCLUDED.salary_currency,
                updated_at = CURRENT_TIMESTAMP
        """)
        statement.close()
    finally:
        connection.close()


def main():
    s3_bucket = required_env("S3_BUCKET")
    bronze_path = optional_env("S3_BRONZE_PATH", s3_path(s3_bucket, "bronze", "jobs"))
    silver_path = optional_env("S3_SILVER_PATH", s3_path(s3_bucket, "silver", "jobs"))
    checkpoint = optional_env("S3_CHECKPOINT_PATH", s3_path(s3_bucket, "checkpoints", "jobs"))

    spark = configure_s3(
        SparkSession.builder.appName("TechJobMarketStreaming")
    ).getOrCreate()
    global spark_session
    spark_session = spark
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("subscribe", "jobs.raw")
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", os.getenv("SPARK_MAX_OFFSETS_PER_TRIGGER", "1000"))
        .load()
    )

    parsed_stream = raw_stream.select(
        col("value").cast("string").alias("raw"),
        from_json(col("value").cast("string"), JOB_SCHEMA).alias("job"),
    )
    invalid_jobs = parsed_stream.where(col("job.id").isNull()).select(
        to_json(struct(
            col("raw"),
            lit("Unable to parse job payload or missing job id").alias("error"),
        )).alias("value")
    )
    jobs = parsed_stream.where(col("job.id").isNotNull()).select("job.*")

    bronze = parsed_stream.select(col("raw").alias("value"))
    bronze_query = (
        bronze.writeStream.format("text")
        .option("path", bronze_path)
        .option("checkpointLocation", checkpoint + "/bronze")
        .outputMode("append")
        .start()
    )

    dlq_query = (
        invalid_jobs.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        .option("topic", "jobs.dlq")
        .option("checkpointLocation", checkpoint + "/dlq")
        .outputMode("append")
        .start()
    )

    silver_query = (
        jobs.writeStream.format("parquet")
        .option("path", silver_path)
        .option("checkpointLocation", checkpoint + "/silver")
        .outputMode("append")
        .start()
    )

    gold_query = (
        jobs.writeStream.foreachBatch(write_gold)
        .option("checkpointLocation", checkpoint + "/gold")
        .outputMode("update")
        .start()
    )

    spark.streams.awaitAnyTermination()
    bronze_query.stop()
    silver_query.stop()
    gold_query.stop()
    dlq_query.stop()
    spark.stop()


if __name__ == "__main__":
    main()
