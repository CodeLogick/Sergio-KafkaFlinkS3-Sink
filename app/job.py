import json
import os
from datetime import datetime

from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

# Utility function for batch number tracking
BATCH_NUMBER_FILE = "batch_number_tracker.json"


def get_next_batch_number(topic, endpoint, group_id):
    """Fetch and increment the batch number for a topic, endpoint, and group_id combination."""
    key = f"{topic}_{endpoint}_{group_id}"
    if os.path.exists(BATCH_NUMBER_FILE):
        with open(BATCH_NUMBER_FILE, "r") as file:
            batch_data = json.load(file)
    else:
        batch_data = {}

    batch_number = batch_data.get(key, 0) + 1
    batch_data[key] = batch_number

    with open(BATCH_NUMBER_FILE, "w") as file:
        json.dump(batch_data, file)

    return str(batch_number)


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


# Main Transformation Logic
def transform_and_sink_data(kafka_endpoint, topic, group_id):
    batch_number = get_next_batch_number(topic, kafka_endpoint, group_id)
    batch_timestamp = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    client_id = f"client_{topic}"
    source_name = f"source_{topic}"
    now = datetime.now()
    year = now.strftime('%Y')  # 'yyyy'
    month = now.strftime('%m')  # 'MM'
    date = now.strftime('%d')  # 'DD'
    hour = now.strftime('%H')  # 'HH'
    minute = now.strftime('%M')
    return f"""
            INSERT INTO iceberg_table
            SELECT
                '{batch_timestamp}.log' AS flink_log_file_name,
                '{batch_timestamp}' AS flink_log_batch_timestamp,
                '{batch_number}' AS flink_log_batch_number,
                '{kafka_endpoint}' AS flink_log_source_endpoint,
                '{client_id}' AS flink_log_source_client_id,
                '{source_name}' AS flink_log_source_name,
                `timestamp` AS flink_log_event_timestamp,
                id AS bln_chg_key,
                CAST(playerId AS INT) AS bln_chg_player_id,
                CAST(portalId AS INT) AS bln_chg_portal_id,
                currency AS bln_chg_currency,
                CAST(realBalance AS DECIMAL(29, 17)) AS bln_chg_real_balance,
                CAST(bonusBalance AS DECIMAL(29, 17)) AS bln_chg_bonus_balance,
                CAST(realBalanceEUR AS DECIMAL(29, 17)) AS bln_chg_real_balance_eur,
                CAST(bonusBalanceEUR AS DECIMAL(29, 17)) AS bln_chg_bonus_balance_eur,
                TO_TIMESTAMP_LTZ(CAST(UNIX_TIMESTAMP(`timestamp`) AS BIGINT), 3) AS bln_chg_timestamp,
                CAST('{year}' AS VARCHAR) AS `year`,
                CAST('{month}' AS VARCHAR) AS `month`,
                CAST('{date}' AS VARCHAR) AS `date`,
                CAST('{hour}' AS VARCHAR) AS `hour`,
                CAST('{minute}' AS VARCHAR) AS `minute`
            FROM kafka_source
            """


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

    statement_set = table_env.create_statement_set()
    statement_set.add_insert_sql(transform_and_sink_data(kafka_endpoint, topic, group_id))
    statement_set.execute().wait()

    # table_env.execute_sql(transform_and_sink_data(kafka_endpoint, topic, group_id))

    # print(f"Starting Flink job for topic '{topic}' with group ID '{group_id}'...")
    # execution_plan = env.get_execution_plan()
    # env.execute("KafkaToS3IcebergJob")
    # print(execution_plan)
