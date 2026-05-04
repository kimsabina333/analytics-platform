import binascii
import hashlib
import hmac
import json
import os
import time
from base64 import urlsafe_b64decode, urlsafe_b64encode

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from backend.services.db_service import DBService, DB_PATH

router = APIRouter()

# Shared DB instance (same app.db used by the cache layer)
_db = DBService(DB_PATH)

SECRET_KEY = os.environ.get(
    "AUTH_SECRET", "analytics-platform-secret-key-change-in-production"
)

# ── Password hashing (PBKDF2-SHA512 + random salt) ────────────────────────────

def _hash_password(password: str) -> str:
    salt = hashlib.sha256(os.urandom(60)).hexdigest().encode("ascii")
    dk = hashlib.pbkdf2_hmac("sha512", password.encode(), salt, 100_000)
    return (salt + binascii.hexlify(dk)).decode("ascii")


def _verify_password(stored: str, provided: str) -> bool:
    salt = stored[:64].encode("ascii")
    stored_hash = stored[64:]
    dk = hashlib.pbkdf2_hmac("sha512", provided.encode(), salt, 100_000)
    return hmac.compare_digest(stored_hash, binascii.hexlify(dk).decode("ascii"))


# ── HMAC-signed token (30-day expiry) ─────────────────────────────────────────

def _make_token(user_id: int, email: str) -> str:
    payload = urlsafe_b64encode(
        json.dumps(
            {"user_id": user_id, "email": email, "exp": time.time() + 86400 * 30}
        ).encode()
    ).decode()
    sig = hmac.new(SECRET_KEY.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return f"{payload}.{sig}"


def decode_token(token: str) -> dict:
    try:
        payload_b64, sig = token.rsplit(".", 1)
        expected = hmac.new(
            SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(sig, expected):
            raise ValueError("bad signature")
        data = json.loads(urlsafe_b64decode(payload_b64 + "=="))
        if data.get("exp", 0) < time.time():
            raise HTTPException(status_code=401, detail="Token expired")
        return data
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")


# ── Schemas ───────────────────────────────────────────────────────────────────

class AuthRequest(BaseModel):
    email: str
    password: str


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/register")
def register(req: AuthRequest):
    email = req.email.lower().strip()
    if not email or "@" not in email:
        raise HTTPException(status_code=400, detail="Invalid email address")
    if len(req.password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    try:
        uid = _db.user_create(email, _hash_password(req.password))
        return {"token": _make_token(uid, email), "email": email}
    except Exception as e:
        if "UNIQUE" in str(e):
            raise HTTPException(status_code=400, detail="Email already registered")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/login")
def login(req: AuthRequest):
    email = req.email.lower().strip()
    user = _db.user_by_email(email)
    if not user or not _verify_password(user["password_hash"], req.password):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    return {"token": _make_token(user["id"], email), "email": email}


@router.post("/change-password")
def change_password(req: ChangePasswordRequest, authorization: str = Header("")):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Not authenticated")
    data = decode_token(authorization[7:])
    user = _db.user_by_email(data["email"])
    if not user or not _verify_password(user["password_hash"], req.current_password):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(req.new_password) < 8:
        raise HTTPException(status_code=400, detail="New password must be at least 8 characters")
    _db.user_update_password(user["id"], _hash_password(req.new_password))
    return {"status": "ok"}
