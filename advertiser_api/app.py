from fastapi import FastAPI, WebSocket, Query, HTTPException, Request, WebSocketDisconnect
from fastapi.responses import PlainTextResponse
import asyncpg
import redis.asyncio as aioredis
import os
import json
import logging
from typing import Optional
from contextlib import asynccontextmanager
from collections import defaultdict
from slowapi import Limiter
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from fastapi.middleware.cors import CORSMiddleware

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Rate Limiting Setup
# Default: 60 requests/minute globally. Adjust as needed.
limiter = Limiter(key_func=get_remote_address, default_limits=["60/minute"])

# Limits for development purpose
MAX_WEBSOCKETS = 2

# State to track active WebSocket connections per ad_id
active_websockets = defaultdict(set)

# Database Configuration
DB_CONFIG = {
    "database": os.getenv("POSTGRES_DB", "adtrack"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin"),
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}

# Redis Configuration
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))

# Define allowed origins
origins = [
    "http://localhost",
    "http://localhost:3000"
]

class Database:
    def __init__(self):
        self.pool: Optional[asyncpg.pool.Pool] = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(**DB_CONFIG)
            logger.info("Connected to PostgreSQL.")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            raise

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed.")

    async def fetch_historical_data(self, ad_id: str, interval: str):
        interval_map = {
            "1h": "1 hour",
            "1d": "1 day",
            "1w": "7 days",
            "1m": "1 month"
        }
        if interval not in interval_map:
            raise ValueError("Invalid interval")

        interval_str = interval_map[interval]
        query = f"""
            SELECT action, COUNT(*) AS count
            FROM ad_events
            WHERE ad_id = $1 AND timestamp >= NOW() - INTERVAL '{interval_str}'
            GROUP BY action
        """
        async with self.pool.acquire() as connection:
            records = await connection.fetch(query, ad_id)
            return records


class RedisClient:
    def __init__(self):
        self.redis: Optional[aioredis.Redis] = None

    async def connect(self):
        try:
            self.redis = aioredis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                decode_responses=True,
                encoding='utf-8',
                max_connections=10
            )
            await self.redis.ping()
            logger.info("Connected to Redis.")
        except Exception as e:
            logger.error(f"Error connecting to Redis: {e}")
            raise

    async def disconnect(self):
        if self.redis:
            await self.redis.close()
            # The following line is optional and depends on the aioredis version
            if hasattr(self.redis, 'connection_pool') and self.redis.connection_pool:
                await self.redis.connection_pool.disconnect()
            logger.info("Redis connection pool closed.")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = Database()
    redis_client = RedisClient()
    app.state.db = db
    app.state.redis_client = redis_client
    await db.connect()
    await redis_client.connect()
    logger.info("All dependencies are ready. Application is starting.")
    try:
        yield
    finally:
        await db.disconnect()
        await redis_client.disconnect()
        logger.info("Shutting down application...")


# Initialize FastAPI with lifespan
app = FastAPI(lifespan=lifespan)

# Attach the limiter to app.state so SlowAPI middleware can access it
app.state.limiter = limiter

# Add SlowAPI middleware after attaching the limiter
app.add_middleware(SlowAPIMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rate limit exception handler
@app.exception_handler(RateLimitExceeded)
async def rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return PlainTextResponse(str(exc), status_code=429)

# Optional: Existing middleware for processing requests
@app.middleware("http")
async def add_process_time_header(request: Request, call_next):
    # This middleware can be used to add custom headers or process the request further
    response = await call_next(request)
    return response

# Endpoint: Fetch Historical Data
@app.get("/api/historical-data")
@limiter.limit("10/minute")  # 10 requests per minute for this endpoint
async def get_historical_data(
    request: Request,  # Added Request parameter for SlowAPI
    ad_id: str,
    interval: str = Query(..., pattern="^(1h|1d|1w|1m)$")
):
    """
    Fetch historical data for a specific ad_id and time interval.
    """
    db: Database = app.state.db
    try:
        records = await db.fetch_historical_data(ad_id, interval)
        if not records:
            logger.warning(f"No historical data found for ad_id: {ad_id}")
            raise HTTPException(status_code=404, detail=f"No historical data found for ad_id: {ad_id}")
        return [{"id": ad_id, "name": record['action'], "value": record['count']} for record in records]
    except ValueError as ve:
        logger.error(f"ValueError: {ve}")
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# Endpoint: Fetch Real-Time Metrics
@app.get("/api/metrics/{ad_id}")
async def get_real_time_metrics(ad_id: str):
    """
    Fetch real-time metrics like CTR, total clicks, views, and cost.
    """
    redis_client: RedisClient = app.state.redis_client
    try:
        metrics = await redis_client.redis.hgetall(f"ad_counts:{ad_id}")
        if not metrics:
            logger.warning(f"No metrics found for ad_id: {ad_id}")
            raise HTTPException(status_code=404, detail=f"No metrics found for ad_id: {ad_id}")
        return {
            "ad_id": ad_id,
            "total_views": int(metrics.get("total_views", 0)),
            "total_clicks": int(metrics.get("total_clicks", 0)),
            "ctr": float(metrics.get("ctr", 0.0)),
            "cumulative_cost": float(metrics.get("cumulative_cost", 0.0))
        }
    except Exception as e:
        logger.error(f"Error fetching metrics: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

# WebSocket Endpoint: Real-Time Data Streaming
@app.websocket("/api/websocket/{ad_id}")
async def websocket_endpoint(websocket: WebSocket, ad_id: str):
    """
    Real-time WebSocket endpoint for streaming data for a specific ad_id.
    """
    redis_client: RedisClient = app.state.redis_client
    if len(active_websockets[ad_id]) >= MAX_WEBSOCKETS:
        await websocket.close(code=4001)
        logger.warning(f"WebSocket connection limit reached for ad_id: {ad_id}")
        return

    await websocket.accept()
    active_websockets[ad_id].add(websocket)
    logger.info(f"WebSocket connected for ad_id: {ad_id}")

    try:
        pubsub = redis_client.redis.pubsub()
        await pubsub.subscribe('realtime-updates')
        logger.info(f"Subscribed to 'realtime-updates' channel for ad_id: {ad_id}")

        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    if data.get("ad_id") == ad_id:
                        await websocket.send_json(data)
                except json.JSONDecodeError:
                    logger.error("Failed to decode JSON message from Redis.")
    except WebSocketDisconnect:
        logger.info(f"WebSocket disconnected for ad_id: {ad_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        active_websockets[ad_id].remove(websocket)
        logger.info(f"WebSocket connection closed for ad_id: {ad_id}")