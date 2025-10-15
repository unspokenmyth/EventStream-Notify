from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import redis.asyncio as redis  # asyncio-compatible Redis client
import asyncio
import json
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = FastAPI()

# Allow requests from React (localhost:3000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # or ["http://localhost:3000"] for stricter setup
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Redis connection
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)
PUBSUB_CHANNEL = "events"

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logging.info("✅ WebSocket connected")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logging.info("❌ WebSocket disconnected")

    async def broadcast(self, message: str):
        for conn in self.active_connections:
            try:
                await conn.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        # 1️⃣ Send last 50 events from Redis on connect
        try:
            keys = await redis_client.keys("event:*")
            keys = sorted(keys)[-50:]  # latest 50 events
            for key in keys:
                ev = await redis_client.get(key)
                if ev:
                    await websocket.send_text(ev)
        except Exception as e:
            logging.error(f"Failed to fetch last events: {e}")

        # 2️⃣ Subscribe to Redis for real-time events
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(PUBSUB_CHANNEL)

        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message and message["type"] == "message":
                await websocket.send_text(message["data"])
            await asyncio.sleep(0.01)  # small sleep to yield control

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logging.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)

async def redis_listener():
    """Optional background listener to log Redis events"""
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(PUBSUB_CHANNEL)
    logging.info(f"Subscribed to Redis channel: {PUBSUB_CHANNEL}")
    while True:
        message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
        if message and message["type"] == "message":
            data = message["data"]
            logging.info(f"📡 Received from Redis: {data}")
            await manager.broadcast(data)
        await asyncio.sleep(0.01)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(redis_listener())
