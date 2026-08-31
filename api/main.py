import asyncio
import logging
import threading
from contextlib import asynccontextmanager
from typing import List

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from config import settings
from schemas import AnomalyAlertSchema
from consumer.stream_processor import StreamProcessor

# Setup Structured Enterprise Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("sentinel.api")

# WebSocket Connection Pool Manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"New WebSocket client connected. Active clients: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info(f"WebSocket client disconnected. Active clients: {len(self.active_connections)}")

    async def broadcast(self, message: dict):
        for connection in list(self.active_connections):
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to client: {e}")
                self.disconnect(connection)

manager = ConnectionManager()
templates = Jinja2Templates(directory="templates")
main_loop = None

def on_anomaly_detected(alert_payload: dict):
    """Bridge thread boundary from Kafka worker to asyncio event loop."""
    if main_loop and main_loop.is_running():
        asyncio.run_coroutine_threadsafe(manager.broadcast(alert_payload), main_loop)

# Modern FastAPI Lifespan Handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    global main_loop
    main_loop = asyncio.get_running_loop()
    
    logger.info("Initializing Kafka Consumer background thread...")
    processor = StreamProcessor(alert_callback=on_anomaly_detected)
    kafka_thread = threading.Thread(target=processor.process_stream, daemon=True)
    kafka_thread.start()
    
    yield  # Application runs while sitting here
    
    logger.info("Shutting down API server...")

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan
)

@app.get("/", response_class=HTMLResponse)
async def get_dashboard(request: Request):
    """Render SOC Dashboard using Jinja2 Template."""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.websocket("/ws/alerts")
async def websocket_endpoint(websocket: WebSocket):
    """Persistent WebSocket endpoint for streaming real-time security alerts."""
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # Keep connection open
    except WebSocketDisconnect:
        manager.disconnect(websocket)