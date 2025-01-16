import json
import os
from pyflink.table import TableEnvironment, EnvironmentSettings
from datetime import datetime

# File to track batch numbers
BATCH_TRACKER_FILE = "batch_tracker.json"

# Initialize TableEnvironment
def setup_environment():
    env_settings = EnvironmentSettings.in_batch_mode()
    return TableEnvironment.create(env_settings)

# Load or initialize batch tracker
def load_batch_tracker():
    if os.path.exists(BATCH_TRACKER_FILE):
        with open(BATCH_TRACKER_FILE, "r") as f:
            return json.load(f)
    return {}

# Save batch tracker
def save_batch_tracker(batch_tracker):
    with open(BATCH_TRACKER_FILE, "w") as f:
        json.dump(batch_tracker, f, indent=4)

# Get the next batch number for a specific topic, endpoint, and group ID
def get_next_batch_number(topic, endpoint, group_id):
    key = f"{topic}_{endpoint}_{group_id}"
    batch_tracker = load_batch_tracker()
    if key in batch_tracker:
        batch_tracker[key] += 1
    else:
        batch_tracker[key] = 1
    save_batch_tracker(batch_tracker)
    return batch_tracker[key]

# Define Kafka Source Table
def define_kafka_source(t_env, topic, endpoint, group_id):
    t_env.execute_sql(f"""
    CREATE TEMPORARY TABLE kafka_input (
        playerId STRING,
        currency STRING,
        portalId STRING,
        realBalance DECIMAL(29, 17),
        bonusBalance DECIMAL(29, 17),
        temp_timestamp STRING,
        id STRING,
        realBalanceEUR DECIMAL(29, 17),
        bonusBalanceEUR DECIMAL(29, 17)
    ) WITH (
        'connector' = 'kafka',
        'topic' = '{topic}',
        'properties.bootstrap.servers' = '{endpoint}',
        'properties.group.id' = '{group_id}',
        'format' = 'json',
        'json.ignore-parse-errors' = 'true'
    )
    """)

# Define Iceberg Sink Table
def define_iceberg_sink(t_env, log_file_name):
    t_env.execute_sql(f"""
    CREATE TABLE iceberg_table (
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
        bln_chg_timestamp TIMESTAMP(3)
    ) WITH (
        'connector' = 'iceberg',
        'catalog-name' = 'hadoop_catalog',
        'warehouse' = 's3://samegrid-test-pub',
        'write.format.default' = 'parquet',
        'write.target-file-size-bytes' = '134217728',
        'write.filename.prefix' = '{log_file_name.replace(".log", "")}'
    )
    """)

# Perform Data Transformation and Insert
def process_and_insert_data(t_env, log_file_name, batch_timestamp, batch_number, kafka_endpoint):
    t_env.execute_sql(f"""
    INSERT INTO iceberg_table
    SELECT
        '{log_file_name}' AS flink_log_file_name,
        '{batch_timestamp}' AS flink_log_batch_timestamp,
        '{batch_number}' AS flink_log_batch_number,
        '{kafka_endpoint}' AS flink_log_source_endpoint,
        'dummy_client_id' AS flink_log_source_client_id,
        'dummy_source_name' AS flink_log_source_name,
        temp_timestamp AS flink_log_event_timestamp,
        id AS bln_chg_key,
        CAST(playerId AS INT) AS bln_chg_player_id,
        CAST(portalId AS INT) AS bln_chg_portal_id,
        currency AS bln_chg_currency,
        realBalance AS bln_chg_real_balance,
        bonusBalance AS bln_chg_bonus_balance,
        realBalanceEUR AS bln_chg_real_balance_eur,
        bonusBalanceEUR AS bln_chg_bonus_balance_eur,
        TO_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP(CAST(temp_timestamp AS STRING), 'yyyy-MM-dd''T''HH:mm:ssXXX'))) AS bln_chg_timestamp
    FROM kafka_input
    """)

# Main Function
def main():
    # Step 1: Setup
    t_env = setup_environment()

    # Step 2: Generate Dynamic Values
    topic = "balance_changes"
    endpoint = "aba0379f767fc48a3b6d0ae99e95a4d7-1436743986.eu-west-1.elb.amazonaws.com:9094"
    group_id = "flink-group"
    batch_number = get_next_batch_number(topic, endpoint, group_id)
    batch_timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')
    log_file_name = f"{datetime.utcnow().strftime('%Y-%m-%d_%H-%M-%S')}.log"

    # Step 3: Define Source and Sink
    define_kafka_source(t_env, topic, endpoint, group_id)
    define_iceberg_sink(t_env, log_file_name)

    # Step 4: Process Data
    process_and_insert_data(t_env, log_file_name, batch_timestamp, batch_number, endpoint)

    print(f'Successfully saved the batch.')

if __name__ == "__main__":
    main()
