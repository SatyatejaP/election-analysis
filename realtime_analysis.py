import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from utils import createKafkaTopic, postgres_cnf, kafka_cnf

if __name__ == '__main__':
    # creates voting kafka topic if not exists
    createKafkaTopic(kafka_cnf['voting_topic'])
    createKafkaTopic(kafka_cnf['aggregated_votes_topic'])
    cwd = os.getcwd()
    spark = (SparkSession.builder
             .appName('ElectionAnalysis')
             .master('local[*]')  # Use local Spark execution with all available cores
             .config('spark.jars.packages',
                     'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,'
                     'org.apache.kafka:kafka-clients:3.5.0,org.postgresql:postgresql:42.7.1')
             .config('spark.driver.extraClassPath',
                     f'{cwd}/jars/postgresql-42.7.1.jar')
             .config('spark.sql.adaptive.enabled', 'false')
             .getOrCreate())

    vote_schema = """
        id STRING,
        name STRING,
        gender STRING,
        age INT,
        city STRING,
        state STRING,
        pincode STRING,
        phone_number STRING,
        constituency_id STRING,
        candidate_id STRING,
        voted_time INT
    """

    url = f"jdbc:postgresql://{postgres_cnf['host']}:{postgres_cnf['port']}/{postgres_cnf['database']}"
    properties = {"user": f"{postgres_cnf['user']}", "password": f"{postgres_cnf['password']}"}

    candidates_df = spark.read.jdbc(url=url, table=f"""(
        SELECT can.id as candidate_id, can.name as candidate_name, can.gender, can.age,can.photo_url, par.name as party_name, con.name as constituency_name, con.state as state
        FROM {postgres_cnf['schema']}.candidates can
        JOIN {postgres_cnf['schema']}.constituencies con ON can.constituency_id = con.id
        JOIN {postgres_cnf['schema']}.parties par ON can.party_id = par.id
        ) as candidates_data""", properties=properties)

    readVotes = (spark.readStream.format("kafka")
                 .option("kafka.bootstrap.servers", kafka_cnf['bootstrap_servers'])
                 .option("subscribe", kafka_cnf['voting_topic'])
                 .option("startingOffsets", "earliest")
                 .load().selectExpr("CAST(value AS STRING)")
                 .select(from_json(col("value"), vote_schema).alias("resources"))
                 .select("resources.*"))

    votes_df = readVotes.withColumn("voted_time_stamp", to_timestamp(col("voted_time"))) \
        .drop("voted_time").withColumnRenamed("voted_time_stamp", "voted_time") \
        .withWatermark("voted_time", "1 minute")

    votes_polled_per_constituency = votes_df.groupby("constituency_id").agg({'id': 'count'})

    votes_per_candidate = votes_df.groupby("constituency_id", "candidate_id").agg(
        {'id': 'count'}).withColumnRenamed("count(id)", "vote_count").join(candidates_df, "candidate_id").select(
        'constituency_id', 'candidate_id', 'vote_count', 'party_name', 'constituency_name', 'candidate_name', 'state')


    def process_batch(df, epoch_id):
        df.show(truncate=False)


    def write_votes_per_candidate_kafka(df, epoch_id):
        df.selectExpr("to_json(struct(*)) AS value").write.format("kafka").option("kafka.bootstrap.servers",
                                                                                  kafka_cnf['bootstrap_servers']) \
            .option("topic", kafka_cnf['aggregated_votes_topic']) \
            .save()


    # votes_per_candidate.writeStream.outputMode("update").foreachBatch(
    #     process_batch).start()

    votes_per_candidate.writeStream.outputMode("update").foreachBatch(write_votes_per_candidate_kafka).start()

    spark.streams.awaitAnyTermination()
