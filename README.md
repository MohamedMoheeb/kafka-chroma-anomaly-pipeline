# Real-Time Log Vector Ingestion & Anomaly Detection Pipeline

An enterprise-grade, asynchronous log streaming architecture that ingests high-velocity raw log strings, transforms unstructured log data into high-dimensional vector embeddings, indexes events inside ChromaDB, and calculates vector distance anomalies to issue real-time WebSocket alerts.

## System Architecture

```text
  ┌────────────────┐      ┌────────────────┐      ┌──────────────────────┐
  │ Log Producer   │ ───► │ Apache Kafka   │ ───► │ Asynchronous Stream  │
  │ (Faker Engine) │      │ (Topic: logs)  │      │ Consumer Service     │
  └────────────────┘      └────────────────┘      └──────────┬───────────┘
                                                             │
                                                  Embedding  │ Vector Upsert
                                                  Generation │ & Distance Check
                                                             ▼
  ┌────────────────┐      ┌────────────────┐      ┌──────────────────────┐
  │ Live Dashboard │ ◄─── │ FastAPI Gateway│ ◄─── │ ChromaDB Engine      │
  │ (WebSockets)   │      │ (Alert Broker) │      │ (Cosine Space Index) │
  └────────────────┘      └────────────────┘      └──────────────────────┘
```

## System Design & Technical Justification ("The Why")

* **Vector Distance vs. Rule-Based Regex**: Legacy Security Information and Event Management (SIEM) tools rely on rigid regex rules that fail against zero-day variations, modified payloads, or novel prompt injection vectors. Embedding raw log strings into vector space captures semantic payload distance, exposing malicious activity without requiring matching string definitions.
* **Apache Kafka Over Message Queues**: Selected for log ingestion because it guarantees message durability, low-latency log partitioning, and high throughput for concurrent telemetry feeds.
* **Local ChromaDB Cosine Indexing**: Configured with persistent local vector storage to allow continuous background upserts and instant distance lookups without incurring external API costs.

## Production Controls & Reliability

* **Thread-Safe WebSocket Event Routing**: Utilizes `asyncio.run_coroutine_threadsafe` to safely bridge synchronous Kafka background worker threads with FastAPI's asynchronous WebSocket server.
* **Bounded Vector Indexing**: Configured index space strategies in ChromaDB (`hnsw:space: cosine`) optimize distance queries over unstructured parameters.
* **Zero-Downtime Pipeline Degradation**: Exceptions during continuous vector ingestion are isolated inside worker frames to prevent queue consumer failure.

## Quickstart Guide

1. **Launch Infrastructure**
   ```bash
   docker-compose up -d
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Run Application Server (Consumer + WebSocket API)**
   ```bash
   uvicorn api.main:app --host 0.0.0.0 --port 8000
   ```

4. **Trigger Producer (Separate Terminal)**
   ```bash
   python -m producer.log_generator
   ```

5. **Access Live Telemetry**
   Open `http://localhost:8000` in your browser to view incoming real-time security anomalies pushed over WebSockets.
