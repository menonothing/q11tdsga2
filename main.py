import base64
import time
import uuid
from collections import defaultdict, deque
from threading import Lock

from fastapi import Body, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

T = 46    # total orders
R = 17    # requests per window
WINDOW = 10  # seconds

app = FastAPI()
lock = Lock()

orders_by_key = {}                 # idempotency key -> order
client_hits = defaultdict(deque)   # client id -> timestamps


# ---------- Rate limiting (middleware) ----------
@app.middleware("http")
async def rate_limiter(request: Request, call_next):
    client_id = request.headers.get("x-client-id") or (
        request.client.host if request.client else "anonymous"
    )
    now = time.time()
    with lock:
        hits = client_hits[client_id]
        while hits and now - hits[0] >= WINDOW:
            hits.popleft()
        if len(hits) >= R:
            retry_after = max(1, int(WINDOW - (now - hits[0])) + 1)
            return JSONResponse(
                status_code=429,
                content={"detail": "Too many requests"},
                headers={"Retry-After": str(retry_after)},
            )
        hits.append(now)
    return await call_next(request)


# ---------- CORS (added AFTER the limiter so it wraps it) ----------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["Retry-After"],
)


# ---------- 1. Idempotent POST ----------
@app.post("/orders", status_code=201)
def create_order(
    body: dict = Body(default=None),
    idempotency_key: str = Header(default=None),
):
    if not idempotency_key:
        raise HTTPException(status_code=400, detail="Idempotency-Key header required")
    with lock:
        if idempotency_key in orders_by_key:
            return orders_by_key[idempotency_key]   # same order again
        order = {"id": str(uuid.uuid4()), "details": body or {}}
        orders_by_key[idempotency_key] = order
        return order


# ---------- 2. Cursor pagination ----------
def encode(n: int) -> str:
    return base64.urlsafe_b64encode(str(n).encode()).decode()

def decode(c: str) -> int:
    try:
        return int(base64.urlsafe_b64decode(c.encode()).decode())
    except Exception:
        raise HTTPException(status_code=400, detail="Bad cursor")

@app.get("/orders")
def list_orders(limit: int = Query(10, ge=1, le=100), cursor: str = None):
    last_seen = decode(cursor) if cursor else 0
    start = last_seen + 1
    end = min(start + limit - 1, T)
    items = [{"id": i, "name": f"Order {i}"} for i in range(start, end + 1)]
    next_cursor = encode(end) if end < T else None
    return {"items": items, "next_cursor": next_cursor}


@app.get("/")
def root():
    return {"status": "ok"}