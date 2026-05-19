import logging
import os
import time
from pathlib import Path
from typing import Optional

import httpx
from dotenv import load_dotenv
from fastapi import HTTPException, Request

from .store_utils import get_request_user_id, set_request_user_id

logger = logging.getLogger(__name__)

_TOKEN_CACHE: dict[str, tuple[str, float]] = {}
_CACHE_TTL_SECONDS = 300
_SUPABASE_URL = ""
_SUPABASE_ANON_KEY = ""


def _load_supabase_config_once():
  global _SUPABASE_URL, _SUPABASE_ANON_KEY
  if _SUPABASE_URL and _SUPABASE_ANON_KEY:
    return

  load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")
  _SUPABASE_URL = (os.getenv("SUPABASE_URL") or os.getenv("VITE_SUPABASE_URL") or "").strip().rstrip("/")
  _SUPABASE_ANON_KEY = (os.getenv("SUPABASE_ANON_KEY") or os.getenv("VITE_SUPABASE_ANON_KEY") or "").strip()


def _supabase_config() -> tuple[str, str]:
  _load_supabase_config_once()
  return _SUPABASE_URL, _SUPABASE_ANON_KEY


def _extract_bearer_token(request: Request) -> Optional[str]:
  auth_header = request.headers.get("Authorization", "")
  if not auth_header:
    logger.warning("[_extract_bearer_token] No Authorization header found")
    return None
  parts = auth_header.split(" ", 1)
  if len(parts) != 2 or parts[0].lower() != "bearer":
    logger.warning(f"[_extract_bearer_token] Invalid header format: {parts[0] if parts else 'EMPTY'}")
    return None
  token = parts[1].strip()
  if token:
    logger.info(f"[_extract_bearer_token] Successfully extracted token: {token[:20]}...")
  return token or None


async def resolve_user_id_from_request(request: Request, required: bool = True) -> Optional[str]:
  logger.info("[resolve_user_id_from_request] Starting token resolution...")
  token = _extract_bearer_token(request)
  if not token:
    logger.warning("[resolve_user_id_from_request] No token extracted from headers")
    if required:
      raise HTTPException(status_code=401, detail="Missing Authorization bearer token")
    return None

  now = time.time()
  cached = _TOKEN_CACHE.get(token)
  if cached and cached[1] > now:
    logger.info(f"[resolve_user_id_from_request] Using cached token for user: {cached[0]}")
    return cached[0]

  supabase_url, supabase_anon_key = _supabase_config()
  if not supabase_url or not supabase_anon_key:
    logger.error("[resolve_user_id_from_request] Supabase config missing")
    if required:
      raise HTTPException(
        status_code=500,
        detail="Backend missing SUPABASE_URL or SUPABASE_ANON_KEY",
      )
    return None

  logger.info(f"[resolve_user_id_from_request] Validating token with Supabase at {supabase_url}")
  try:
    async with httpx.AsyncClient(timeout=8.0) as client:
      response = await client.get(
        f"{supabase_url}/auth/v1/user",
        headers={
          "Authorization": f"Bearer {token}",
          "apikey": supabase_anon_key,
        },
      )
      logger.info(f"[resolve_user_id_from_request] Supabase response status: {response.status_code}")
  except httpx.HTTPError as exc:
    logger.error(f"[resolve_user_id_from_request] Failed to validate with Supabase: {exc}")
    if required:
      raise HTTPException(status_code=401, detail="Unable to validate access token")
    return None

  if response.status_code != 200:
    logger.error(f"[resolve_user_id_from_request] Token validation failed: {response.status_code} - {response.text}")
    if required:
      raise HTTPException(status_code=401, detail="Invalid or expired access token")
    return None

  payload = response.json() if response.content else {}
  user_id = payload.get("id")
  if not user_id:
    logger.error(f"[resolve_user_id_from_request] No user_id in payload: {payload}")
    if required:
      raise HTTPException(status_code=401, detail="Invalid user token payload")
    return None

  logger.info(f"[resolve_user_id_from_request] Successfully resolved user_id: {user_id}")
  _TOKEN_CACHE[token] = (user_id, now + _CACHE_TTL_SECONDS)
  return user_id


async def require_authenticated_user(request: Request) -> str:
  current_user_id = get_request_user_id()
  if current_user_id:
    logger.info(f"Using cached user_id: {current_user_id}")
    return current_user_id
  logger.info("Resolving user_id from Supabase token...")
  user_id = await resolve_user_id_from_request(request, required=True)
  logger.info(f"Resolved user_id from token: {user_id}")
  set_request_user_id(user_id)
  return user_id
