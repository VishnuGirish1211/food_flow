"""Auth service API routes.

POST /register  — Create a new user account
POST /login     — Authenticate and receive JWT tokens
POST /refresh   — Exchange a refresh token for a new access token
"""

from datetime import datetime, timedelta, timezone

import structlog
from fastapi import APIRouter, HTTPException, Request

from auth import (
    create_access_token,
    create_refresh_token,
    hash_password,
    hash_refresh_token,
    verify_password,
    REFRESH_TOKEN_EXPIRE_DAYS,
)
from models import (
    AuthResponse,
    LoginRequest,
    RefreshRequest,
    RegisterRequest,
    UserResponse,
)

logger = structlog.get_logger()
router = APIRouter()


@router.post("/register", status_code=201, response_model=UserResponse)
async def register(request: Request, body: RegisterRequest):
    """Register a new user.

    Returns 409 if the email is already taken.
    """
    pool = request.app.state.db_pool

    password_hash = hash_password(body.password)

    async with pool.acquire() as conn:
        # Check if email already exists
        existing = await conn.fetchval(
            "SELECT id FROM users WHERE email = $1", body.email
        )
        if existing:
            raise HTTPException(status_code=409, detail="Email already registered")

        row = await conn.fetchrow(
            """
            INSERT INTO users (email, password_hash)
            VALUES ($1, $2)
            RETURNING id, email, created_at
            """,
            body.email,
            password_hash,
        )

    logger.info("user_registered", user_id=str(row["id"]), email=body.email)

    return UserResponse(
        id=str(row["id"]),
        email=row["email"],
        created_at=row["created_at"].isoformat(),
    )


@router.post("/login", response_model=AuthResponse)
async def login(request: Request, body: LoginRequest):
    """Authenticate user and return access + refresh tokens.

    Returns 401 if credentials are invalid.
    """
    pool = request.app.state.db_pool

    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, email, password_hash FROM users WHERE email = $1",
            body.email,
        )

    if not row or not verify_password(body.password, row["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    user_id = str(row["id"])
    access_token = create_access_token(user_id, row["email"])
    refresh_token = create_refresh_token()
    refresh_hash = hash_refresh_token(refresh_token)

    # Store refresh token hash in DB
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
            VALUES ($1, $2, $3)
            """,
            row["id"],
            refresh_hash,
            datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
        )

    logger.info("user_logged_in", user_id=user_id)

    return AuthResponse(access_token=access_token, refresh_token=refresh_token)


@router.post("/refresh", response_model=AuthResponse)
async def refresh(request: Request, body: RefreshRequest):
    """Exchange a valid refresh token for new access + refresh tokens.

    The old refresh token is deleted (rotation) and a new pair is issued.
    Returns 401 if the refresh token is invalid or expired.
    """
    pool = request.app.state.db_pool
    token_hash = hash_refresh_token(body.refresh_token)

    async with pool.acquire() as conn:
        async with conn.transaction():
            row = await conn.fetchrow(
                """
                SELECT rt.id, rt.user_id, rt.expires_at, u.email
                FROM refresh_tokens rt
                JOIN users u ON u.id = rt.user_id
                WHERE rt.token_hash = $1
                """,
                token_hash,
            )

            if not row:
                raise HTTPException(status_code=401, detail="Invalid refresh token")

            if row["expires_at"] < datetime.now(timezone.utc):
                # Delete expired token
                await conn.execute(
                    "DELETE FROM refresh_tokens WHERE id = $1", row["id"]
                )
                raise HTTPException(status_code=401, detail="Refresh token expired")

            # Delete old refresh token (rotation)
            await conn.execute(
                "DELETE FROM refresh_tokens WHERE id = $1", row["id"]
            )

            # Issue new tokens
            user_id = str(row["user_id"])
            new_access_token = create_access_token(user_id, row["email"])
            new_refresh_token = create_refresh_token()
            new_refresh_hash = hash_refresh_token(new_refresh_token)

            await conn.execute(
                """
                INSERT INTO refresh_tokens (user_id, token_hash, expires_at)
                VALUES ($1, $2, $3)
                """,
                row["user_id"],
                new_refresh_hash,
                datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS),
            )

    logger.info("token_refreshed", user_id=user_id)

    return AuthResponse(
        access_token=new_access_token, refresh_token=new_refresh_token
    )
