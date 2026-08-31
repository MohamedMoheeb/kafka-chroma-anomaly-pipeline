import json
import uuid
import numpy as np
import chromadb
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer

# Configuration
KAFKA_TOPIC = "system_logs"
BOOTSTRAP_SERVERS = ["localhost:9092"]
DISTANCE_THRESHOLD = 0.65  # Cosine distance cutoff for anomalies

class StreamProcessor:
    def __init__(self, alert_callback=None):
        print("[*] Initializing Embeddings Model (all-MiniLM-L6-v2)...")
        self.model = SentenceTransformer("all-MiniLM-L6-v2")
        
        print("[*] Setting up ChromaDB persistence...")
        self.chroma_client = chromadb.PersistentClient(path="./chroma_db")
        self.collection = self.chroma_client.get_or_create_collection(
            name="log_vectors",
            metadata={"hnsw:space": "cosine"}
        )
        self.alert_callback = alert_callback
        self.baseline_embeddings = None
        self._seed_baseline()

    def _seed_baseline(self):
        """Seed normal operation vector baseline for zero-shot distance checking."""
        normal_samples = [
            "GET /api/v1/user/profile HTTP/1.1 | Status: 200 | Client: 192.168.1.1 | Payload: query_param=test",
            "POST /api/v1/auth/login HTTP/1.1 | Status: 200 | Client: 10.0.0.5 | Payload: auth=success",
            "GET /api/v1/products/list HTTP/1.1 | Status: 200 | Client: 172.16.0.2 | Payload: page=1",
            "GET /healthz HTTP/1.1 | Status: 200 | Client: 127.0.0.1 | Payload: ping"
        ]
        self.baseline_embeddings = self.model.encode(normal_samples)

    def calculate_min_distance(self, embedding: np.ndarray) -> float:
        """Calculate minimum cosine distance relative to expected operational baselines."""
        # Cosine distance = 1 - cosine similarity
        dot_product = np.dot(self.baseline_embeddings, embedding)
        norms = np.linalg.norm(self.baseline_embeddings, axis=1) * np.linalg.norm(embedding)
        similarities = dot_product / (norms + 1e-10)
        distances = 1.0 - similarities
        return float(np.min(distances))

    def process_stream(self):
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=BOOTSTRAP_SERVERS,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="log-anomaly-group"
        )
        print(f"[*] Stream Consumer active. Listening on '{KAFKA_TOPIC}'...")

        for msg in consumer:
            log_data = msg.value
            log_text = log_data["formatted_text"]
            
            # 1. Generate Vector Embedding
            embedding = self.model.encode(log_text).tolist()
            doc_id = str(uuid.uuid4())
            
            # 2. Vector Index Upsert into ChromaDB
            self.collection.add(
                ids=[doc_id],
                embeddings=[embedding],
                metadatas=[{
                    "timestamp": log_data["timestamp"],
                    "ip": log_data["ip"],
                    "endpoint": log_data["endpoint"],
                    "status_code": log_data["status_code"]
                }],
                documents=[log_text]
            )
            
            # 3. Anomaly Evaluation
            min_dist = self.calculate_min_distance(np.array(embedding))
            
            if min_dist > DISTANCE_THRESHOLD:
                alert_payload = {
                    "alert": "ANOMALY_DETECTED",
                    "distance": round(min_dist, 4),
                    "raw_log": log_text,
                    "metadata": log_data
                }
                print(f"[!] ALARM (Dist: {min_dist:.4f}): {log_text}")
                if self.alert_callback:
                    self.alert_callback(alert_payload)

if __name__ == "__main__":
    processor = StreamProcessor()
    processor.process_stream()