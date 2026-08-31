import asyncio
import json
from typing import List
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from consumer.stream_processor import StreamProcessor
import threading

app = FastAPI(
    title="Real-Time Log Vector Anomaly Detection API",
    version="1.0.0"
)

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                pass

manager = ConnectionManager()
loop = asyncio.get_event_loop()

def on_anomaly_detected(alert_payload: dict):
    # Route thread-safe anomaly notifications into FastAPI's async loop
    asyncio.run_coroutine_threadsafe(manager.broadcast(alert_payload), loop)

@app.on_event("startup")
def start_kafka_consumer_thread():
    processor = StreamProcessor(alert_callback=on_anomaly_detected)
    thread = threading.Thread(target=processor.process_stream, daemon=True)
    thread.start()

@app.get("/")
async def get_dashboard():
    return HTMLResponse("""
    <!DOCTYPE html>
    <html>
        <head>
            <title>Real-Time Anomaly Monitor</title>
            <style>
                body { font-family: monospace; background-color: #1a1a1a; color: #00ff00; padding: 20px; }
                h1 { color: #fff; }
                #alerts { border: 1px solid #333; height: 400px; overflow-y: scroll; padding: 10px; background: #000; }
                .alert-item { color: #ff4444; margin-bottom: 8px; border-bottom: 1px solid #222; padding-bottom: 4px; }
            </style>
        </head>
        <body>
            <h1>Real-Time Log Anomaly Stream (WebSockets)</h1>
            <div id="alerts"></div>
            <script>
                var ws = new WebSocket("ws://localhost:8000/ws/alerts");
                ws.onmessage = function(event) {
                    var alerts = document.getElementById('alerts');
                    var data = JSON.parse(event.data);
                    var item = document.createElement('div');
                    item.className = 'alert-item';
                    item.innerText = "[" + data.metadata.timestamp + "] DISTANCE: " + data.distance + " | LOG: " + data.raw_log;
                    alerts.prepend(item);
                };
            </script>
        </body>
    </html>
    """)

@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)