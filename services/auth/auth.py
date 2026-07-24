"""JWT and password hashing utilities for the Auth service.

Uses bcrypt for password hashing and PyJWT for token creation/verification.
JWT secret is shared via environment variable with services that need
to verify tokens (e.g., Order service).
"""

import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone

import bcrypt
import jwt

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-in-production")
JWT_ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 15
REFRESH_TOKEN_EXPIRE_DAYS = 7


def hash_password(password: str) -> str:
    """Hash a plaintext password with bcrypt."""
    return bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")


def verify_password(password: str, password_hash: str) -> bool:
    """Verify a plaintext password against a bcrypt hash."""
    return bcrypt.checkpw(password.encode("utf-8"), password_hash.encode("utf-8"))


def create_access_token(user_id: str, email: str) -> str:
    """Create a short-lived JWT access token (15 min)."""
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES),
        "iat": datetime.now(timezone.utc),
        "type": "access",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_refresh_token() -> str:
    """Create a cryptographically random refresh token string."""
    return secrets.token_urlsafe(64)


def hash_refresh_token(token: str) -> str:
    """Hash a refresh token for database storage (SHA-256, not bcrypt).

    Refresh tokens are high-entropy random strings, so SHA-256 is
    sufficient. bcrypt would be unnecessarily slow for token lookups.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def decode_access_token(token: str) -> dict:
    """Decode and verify a JWT access token.

    Raises jwt.ExpiredSignatureError or jwt.InvalidTokenError on failure.
    """
    return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
