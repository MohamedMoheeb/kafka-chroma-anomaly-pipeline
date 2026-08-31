import os
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    PROJECT_NAME: str = "Sentinel Real-Time Log Anomaly Detector"
    VERSION: str = "1.0.0"
    
    # Kafka Configurations
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "system_logs")
    KAFKA_CONSUMER_GROUP: str = "log-anomaly-group"
    
    # ML & Vector DB Configurations
    EMBEDDING_MODEL_NAME: str = "all-MiniLM-L6-v2"
    CHROMA_PERSIST_DIR: str = "./chroma_db"
    DISTANCE_THRESHOLD: float = 0.65  # Cosine distance anomaly cutoff

    class Config:
        env_file = ".env"

settings = Settings()