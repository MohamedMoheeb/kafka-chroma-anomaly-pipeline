from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class LogEntrySchema(BaseModel):
    timestamp: str
    log_type: str
    ip: str
    method: str
    endpoint: str
    status_code: int
    payload: str
    formatted_text: str

class AnomalyAlertSchema(BaseModel):
    alert_type: str = Field(default="ANOMALY_DETECTED")
    distance: float
    raw_log: str
    metadata: Dict[str, Any]