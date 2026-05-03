#FastAPI 推理服务。它启动后会加载训练好的模型，然后提供接口

import json
import os
from pathlib import Path
from typing import Optional

import joblib
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

try:
    from .features import build_features
except ImportError:
    from features import build_features


MODEL_PATH = Path(os.getenv("MODEL_PATH", "ml/models/video_views_model.joblib"))
METRICS_PATH = Path(os.getenv("METRICS_PATH", "ml/models/metrics.json"))

app = FastAPI(title="YouTube Video Views Model", version="0.1.0")
model = None
metrics = {}


class PredictionRequest(BaseModel):
    video_id: Optional[str] = None
    video_title: Optional[str] = None
    upload_date: str = Field(..., examples=["2026-05-01T00:00:00Z"])
    duration: str = Field(..., examples=["PT5M30S", "00:05:30"])
    likes_count: int = Field(0, ge=0)
    comments_count: int = Field(0, ge=0)


class PredictionResponse(BaseModel):
    predicted_views: int
    model_path: str


@app.on_event("startup")
def load_model():
    global model, metrics
    if not MODEL_PATH.exists():
        raise RuntimeError(f"Model file not found: {MODEL_PATH}")

    model = joblib.load(MODEL_PATH)
    if METRICS_PATH.exists():
        metrics = json.loads(METRICS_PATH.read_text(encoding="utf-8"))


@app.get("/health")
def health():
    return {
        "status": "ok" if model is not None else "model_not_loaded",
        "model_path": str(MODEL_PATH),
    }


@app.get("/metrics")
def model_metrics():
    return metrics


@app.post("/predict", response_model=PredictionResponse)
def predict(payload: PredictionRequest):
    if model is None:
        raise HTTPException(status_code=503, detail="Model is not loaded.")

    row = pd.DataFrame(
        [
            {
                "video_id": payload.video_id,
                "video_title": payload.video_title,
                "upload_date": payload.upload_date,
                "duration": payload.duration,
                "likes_count": payload.likes_count,
                "comments_count": payload.comments_count,
            }
        ]
    )
    features = build_features(row)
    predicted_views = max(0, int(round(model.predict(features)[0])))

    return PredictionResponse(
        predicted_views=predicted_views,
        model_path=str(MODEL_PATH),
    )
