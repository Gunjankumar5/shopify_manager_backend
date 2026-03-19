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
    return None
  parts = auth_header.split(" ", 1)
  if len(parts) != 2 or parts[0].lower() != "bearer":
    return None
  token = parts[1].strip()
  return token or None


async def resolve_user_id_from_request(request: Request, required: bool = True) -> Optional[str]:
  token = _extract_bearer_token(request)
  if not token:
    if required:
      raise HTTPException(status_code=401, detail="Missing Authorization bearer token")
    return None

  now = time.time()
  cached = _TOKEN_CACHE.get(token)
  if cached and cached[1] > now:
    return cached[0]

  supabase_url, supabase_anon_key = _supabase_config()
  if not supabase_url or not supabase_anon_key:
    logger.error("Supabase auth config missing on backend")
    if required:
      raise HTTPException(
        status_code=500,
        detail="Backend missing SUPABASE_URL or SUPABASE_ANON_KEY",
      )
    return None

  try:
    async with httpx.AsyncClient(timeout=8.0) as client:
      response = await client.get(
        f"{supabase_url}/auth/v1/user",
        headers={
          "Authorization": f"Bearer {token}",
          "apikey": supabase_anon_key,
        },
      )
  except httpx.HTTPError as exc:
    logger.warning(f"Failed to validate Supabase token: {exc}")
    if required:
      raise HTTPException(status_code=401, detail="Unable to validate access token")
    return None

  if response.status_code != 200:
    if required:
      raise HTTPException(status_code=401, detail="Invalid or expired access token")
    return None

  payload = response.json() if response.content else {}
  user_id = payload.get("id")
  if not user_id:
    if required:
      raise HTTPException(status_code=401, detail="Invalid user token payload")
    return None

  _TOKEN_CACHE[token] = (user_id, now + _CACHE_TTL_SECONDS)
  return user_id


async def require_authenticated_user(request: Request) -> str:
  current_user_id = get_request_user_id()
  if current_user_id:
    return current_user_id
  user_id = await resolve_user_id_from_request(request, required=True)
  set_request_user_id(user_id)
  return user_id
