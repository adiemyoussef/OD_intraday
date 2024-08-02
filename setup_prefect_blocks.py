from prefect.blocks.system import JSON
from prefect.blocks.system import Secret

# Database Credentials Block
db_credentials = JSON(
    value={
        "host": "db-mysql-nyc3-94751-do-user-12097851-0.b.db.ondigitalocean.com",
        "port": 25060,
        "user": "doadmin",
        "password": "AVNS_lRx29D8jydyJ2lmQhan",
        "database": "defaultdb",
        "driver": "mysql+pymysql"
    }
)
db_credentials.save("database-credentials")

# SFTP Credentials Block
sftp_credentials = JSON(
    value={
        "host": "sftp.datashop.livevol.com",
        "port": 22,
        "username": "contact_optionsdepth_com",
        "password": "Salam123+-",
        "directory": "/subscriptions/order_000059435/item_000068201"
    }
)
sftp_credentials.save("sftp-credentials")

# RabbitMQ Configuration Block
rabbitmq_config = JSON(
    value={
        "host": "64.225.63.198",
        "port": 5672,
        "user": "optionsdepth",
        "password": "Salam123+",
        "cboe_queue": "youssef_local",
        "heartbeat_queue": "heartbeats",
        "queue_size_alert_threshold": 1000,
        "heartbeat_interval": 60,
        "clear_heartbeat_interval": 3600,
        "max_runtime": 3600,
        "max_ack_retries": 3
    }
)
rabbitmq_config.save("rabbitmq-config")

# Digital Ocean Spaces Configuration Block
do_spaces_config = JSON(
    value={
        "url": "https://nyc3.digitaloceanspaces.com",
        "key": "DO00EMRQHFB38ZQZRGA7",
        "secret": "gHFYFlzQ1T73gAY7BKRNtgx5hlEMMAB2YZIv+YyQUps",
        "bucket": "intraday"
    }
)
do_spaces_config.save("do-spaces-config")

# Discord Webhook URL (as a Secret)
discord_webhook = Secret(value="https://discord.com/api/webhooks/1251013946111164436/VN55yOK-ntil-PnZn1gzWHzKwzDklwIh6fVspA_I8MCCaUnG-hsRsrP1t_WsreGHIity")
discord_webhook.save("discord-webhook-url")

# Other Configuration Block (for non-sensitive data)
other_config = JSON(
    value={
        "option_symbols_to_process": ["SPX", "SPXW"],
        "csv_chunksize": 100000,
        "sftp_base_sleep_time": 10,
        "sftp_reduced_sleep_time": 5,
        "sftp_expectation_window": 120,
        "sftp_max_retry_attempts": 3,
        "process_message_queue_retry_delay": 5
    }
)
other_config.save("other-config")

print("Prefect Blocks have been created and saved.")