from enum import Enum

class ConnectionType(Enum):
    s3: str = "S3"
    postgresql: str = "PostgreSQL"
    oracle: str = "Oracle"
    streamset: str = "Streamset"
    kafka: str = "Kafka"
    rabbit_mq: str = "RabbitMQ"
    apache_hop: str = "Apache Hop"
