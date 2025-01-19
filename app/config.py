from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


# Initialize Environment
def initialize_environment():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
    env.enable_checkpointing(60000)
    table_env = StreamTableEnvironment.create(stream_execution_environment=env)
    return env, table_env


def register_iceberg_catalog(table_env, catalog_name, warehouse_path):
    response = table_env.execute_sql(
        f"""
            CREATE CATALOG IF NOT EXISTS {catalog_name} WITH (
                'type' = 'iceberg',
                'catalog-type' = 'hadoop',
                'warehouse' = '{warehouse_path}',
                'fs.s3a.endpoint' = 's3.amazonaws.com',
                'fs.s3a.path.style.access' = 'true'
            )
            """
    )
    table_env.use_catalog(catalog_name)
    return response


# Define Kafka Source
def define_kafka_source(kafka_endpoint, topic, group_id):
    stmt = f"""
            CREATE TABLE IF NOT EXISTS kafka_source (
                playerId STRING,
                currency STRING,
                portalId STRING,
                realBalance DOUBLE,
                bonusBalance DOUBLE,
                `timestamp` STRING,
                id STRING,
                realBalanceEUR DOUBLE,
                bonusBalanceEUR DOUBLE
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{topic}',
                'properties.bootstrap.servers' = '{kafka_endpoint}',
                'properties.group.id' = '{group_id}',
                'format' = 'json',
                'scan.startup.mode' = 'latest-offset'
            )
            """
    print(stmt)
    return stmt


# Define S3 Sink
def define_s3_sink(s3_bucket):
    stmt = f"""
            CREATE TABLE IF NOT EXISTS iceberg_table (
                flink_log_file_name STRING,
                flink_log_batch_timestamp STRING,
                flink_log_batch_number STRING,
                flink_log_source_endpoint STRING,
                flink_log_source_client_id STRING,
                flink_log_source_name STRING,
                flink_log_event_timestamp STRING,
                bln_chg_key STRING,
                bln_chg_player_id INT,
                bln_chg_portal_id INT,
                bln_chg_currency STRING,
                bln_chg_real_balance DECIMAL(29, 17),
                bln_chg_bonus_balance DECIMAL(29, 17),
                bln_chg_real_balance_eur DECIMAL(29, 17),
                bln_chg_bonus_balance_eur DECIMAL(29, 17),
                bln_chg_timestamp TIMESTAMP(3),
                `year`              VARCHAR,
                `month`             VARCHAR,
                `date`              VARCHAR,
                `hour`              VARCHAR,
                `minute`              VARCHAR
            ) PARTITIONED BY (`year`, `month`, `date`, `hour`, `minute`) WITH (
                'connector' = 'filesystem',
                'path' = '{s3_bucket}',
                'format' = 'iceberg',
                'sink.partition-commit.delay'='1 h',
                'sink.partition-commit.policy.kind'='success-file'
            )
            """
    print(stmt)
    return stmt


# Main Execution
if __name__ == "__main__":
    kafka_endpoint = "aba0379f767fc48a3b6d0ae99e95a4d7-1436743986.eu-west-1.elb.amazonaws.com:9094"
    topic = "balance_changes"
    group_id = "flink_consumer_group"
    s3_bucket = "s3a://samegrid-test-pub"
    warehouse_path = "s3a://samegrid-test-pub/iceberg-warehouse"
    catalog = "iceberg_catalog"

    print(f'Working with following params')
    print(f"Kafka Endpoint: {kafka_endpoint}")
    print(f"Kafka Topic: {topic}")
    print(f"Kafka Group ID: {group_id}")
    print(f"S3 Bucket: {s3_bucket}")
    print(f"Iceberg Warehouse Path: {warehouse_path}")
    print(f"Iceberg Catalog Name: {catalog}")

    env, table_env = initialize_environment()

    print(f'Registering Catalog: {register_iceberg_catalog(table_env, catalog, warehouse_path).print()}')

    print(f'Creating kafka source table = {define_kafka_source(kafka_endpoint, topic, group_id)}')
    response = table_env.execute_sql(define_kafka_source(kafka_endpoint, topic, group_id))
    print(response.print())

    print(f'Creating S3 sink table = {table_env.execute_sql(define_s3_sink(s3_bucket))}')
    response = table_env.execute_sql(define_s3_sink(s3_bucket))
    print(response.print())

