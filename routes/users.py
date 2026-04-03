"""
routes/users.py — Full User Management with Role-Based Access Control
Your schema: users(id, email, full_name, role, is_active, created_at, last_login, avatar_url)
           + user_permissions(user_id, manage_products, delete_products, ...)
"""
from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel
from typing import Optional
import os, logging

logger = logging.getLogger(__name__)
router = APIRouter()

ALL_PERMISSIONS = [
    "manage_products", "delete_products", "manage_collections",
    "manage_inventory", "manage_metafields", "manage_upload",
    "manage_export", "use_ai", "manage_stores", "manage_users",
    "view_analytics",
]


# ── Supabase helpers ──────────────────────────────────────────────────────────

def _get_supabase_admin():
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    if not url or not key:
        raise HTTPException(503, "Supabase not configured")
    return create_client(url, key)


def _get_supabase_anon():
    from supabase import create_client
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_ANON_KEY", "").strip()
    if not url or not key:
        raise HTTPException(503, "Supabase not configured")
    return create_client(url, key)


# ── Auth: extract user_id from Bearer token ───────────────────────────────────

def get_current_user_id(authorization: str = Header(None)) -> str:
    """Extract user UUID from Bearer token via Supabase auth."""
    logger.info(f"[get_current_user_id] Authorization header received: {'YES' if authorization else 'NO'}")
    if not authorization or not authorization.startswith("Bearer "):
        logger.error("[get_current_user_id] Missing or invalid Authorization header")
        raise HTTPException(401, "Missing or invalid Authorization header")
    token = authorization.split(" ", 1)[1]
    logger.info(f"[get_current_user_id] Token extracted: {token[:20]}...")
    try:
        sb = _get_supabase_anon()
        resp = sb.auth.get_user(token)
        if not resp or not resp.user:
            logger.error("[get_current_user_id] No user in response")
            raise HTTPException(401, "Invalid or expired token")
        logger.info(f"[get_current_user_id] User authenticated: {resp.user.id}")
        return str(resp.user.id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[get_current_user_id] Auth error: {e}")
        raise HTTPException(401, "Authentication failed")


def get_current_user_full(authorization: str = Header(None)) -> dict:
    """Returns full user dict including role and permissions. Auto-creates user if missing."""
    try:
        user_id = get_current_user_id(authorization)
        logger.info(f"[get_current_user_full] Authenticated user: {user_id}")
    except HTTPException as e:
        logger.error(f"[get_current_user_full] Auth failed: {e.detail}")
        raise
    
    sb = _get_supabase_admin()
    try:
        # Query users table: 'id' is the primary key (NOT user_id)
        r = sb.table("users").select(
            "id, email, full_name, role, is_active, created_at, last_login, created_by"
        ).eq("id", user_id).execute()
        
        user_data = r.data[0] if r.data else None
        
        # If user doesn't exist, create it from auth data
        if not user_data:
            logger.info(f"[AUTO-CREATE] User {user_id} not found, creating from auth data...")
            try:
                # Get auth user data
                token = authorization.split(" ", 1)[1]
                auth_resp = _get_supabase_anon().auth.get_user(token)
                if not auth_resp or not auth_resp.user:
                    raise HTTPException(401, "Cannot extract auth response")
                auth_user = auth_resp.user
                if not auth_user or not auth_user.email:
                    raise HTTPException(401, "Cannot extract email from auth token")
                
                # Create user record with defaults
                full_name = auth_user.user_metadata.get("full_name", auth_user.email.split("@")[0]) if auth_user.user_metadata else auth_user.email.split("@")[0]
                
                create_resp = sb.table("users").insert({
                    "id": user_id,
                    "email": auth_user.email,
                    "full_name": full_name,
                    "role": "junior",
                    "is_active": True,
                    "created_by": None,
                }).execute()
                
                if create_resp.data:
                    user_data = create_resp.data[0]
                    logger.info(f"[AUTO-CREATE] ✓ Created user {user_id} ({auth_user.email})")
                else:
                    raise Exception("Failed to insert user record")
            except Exception as create_error:
                logger.error(f"[AUTO-CREATE] Failed: {create_error}")
                raise HTTPException(500, f"Failed to create user profile: {create_error}")
        
        user: dict = user_data  # type: ignore
        user["user_id"] = user.get("id")  # Alias for frontend consistency
        logger.info(f"[get_current_user_full] User loaded: {user.get('email')} (role: {user.get('role')})")
        
        # Fetch permissions using user_id foreign key
        p = sb.table("user_permissions").select("*").eq("user_id", user_id).execute()
        if p.data and len(p.data) > 0:
            perm_data = p.data[0]
            user["permissions"] = perm_data  # type: ignore
        else:
            logger.info(f"[AUTO-CREATE] Permissions missing for {user_id}, creating...")
            try:
                defaults = _default_permissions_for_role(str(user.get("role") or "junior"))
                sb.table("user_permissions").insert({"user_id": user_id, **defaults}).execute()
                user["permissions"] = defaults
                logger.info(f"[AUTO-CREATE] ✓ Created permissions for {user_id}")
            except Exception as perm_error:
                logger.warning(f"[AUTO-CREATE] Could not create permissions: {perm_error}")
                user["permissions"] = {k: False for k in ALL_PERMISSIONS}
        
        perms = user.get("permissions", {})
        perm_keys = list(perms.keys()) if isinstance(perms, dict) else []
        logger.info(f"[get_current_user_full] Permissions: {perm_keys}")
        return user
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[get_current_user_full] Database error: {e}")
        raise HTTPException(500, str(e))


def require_admin(authorization: str = Header(None)) -> dict:
    user = get_current_user_full(authorization)
    if user.get("role") != "admin":
        raise HTTPException(403, "Admin access required")
    return user


def require_admin_or_manager(authorization: str = Header(None)) -> dict:
    user = get_current_user_full(authorization)
    if user.get("role") not in ("admin", "manager"):
        raise HTTPException(403, "Admin or Manager access required")
    return user


# ── Pydantic models ───────────────────────────────────────────────────────────

class CreateUserBody(BaseModel):
    email: str
    full_name: str
    role: str = "junior"                        # admin | manager | junior
    permissions: Optional[dict] = {}
    password: Optional[str] = None              # if None, magic link is sent


class UpdatePermissionsBody(BaseModel):
    permissions: dict                           # {permission_key: bool}


class UpdateRoleBody(BaseModel):
    role: str                                   # admin | manager | junior
    admin_email: Optional[str] = None


def _default_permissions_for_role(role: str) -> dict:
    if role == "admin":
        return {k: True for k in ALL_PERMISSIONS}
    if role == "manager":
        perms = {k: True for k in ALL_PERMISSIONS}
        perms["manage_users"] = False
        perms["manage_stores"] = False
        return perms
    return {k: False for k in ALL_PERMISSIONS}


def _get_target_user(sb, user_id: str) -> dict:
    r = sb.table("users").select(
        "id, email, full_name, role, is_active, created_at, last_login, created_by"
    ).eq("id", user_id).execute()
    if not r.data:
        raise HTTPException(404, "User not found")
    return r.data[0]


def _assert_admin_can_manage_target(sb, admin_user: dict, target_user_id: str) -> dict:
    target = _get_target_user(sb, target_user_id)
    if str(target.get("id")) == str(admin_user.get("id")):
        raise HTTPException(400, "You cannot perform this action on yourself")
    if str(target.get("created_by") or "") != str(admin_user.get("id")):
        raise HTTPException(403, "You can only manage users in your own team")
    return target



# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("/me")
def get_me(authorization: str = Header(None)):
    """Get current user's profile + permissions."""
    try:
        user = get_current_user_full(authorization)
        logger.info(f"[GET /me] User {user.get('id')} loaded successfully")
        return user
    except Exception as e:
        logger.error(f"[GET /me] Error: {e}")
        raise


@router.get("/me/permissions")
def get_my_permissions(authorization: str = Header(None)):
    """Shortcut: just the permissions + role (used by frontend for gating)."""
    user = get_current_user_full(authorization)
    return {
        "role":        user.get("role"),
        "is_active":   user.get("is_active"),
        "permissions": user.get("permissions", {}),
    }


@router.get("/")
def list_users(authorization: str = Header(None)):
    """List users in the current admin's team (admin only)."""
    logger.info("[GET /users/] Request received")
    caller = require_admin(authorization)
    logger.info(f"[GET /users/] Caller authorized: {caller.get('id')} role={caller.get('role')}")
    sb = _get_supabase_admin()
    try:
        caller_id = str(caller.get("id"))
        
        # Fetch team members created by this admin (excludes old orphaned users)
        r = sb.table("users").select(
            "id, email, full_name, role, is_active, created_at, last_login, created_by"
        ).eq("created_by", caller_id).order("created_at", desc=False).execute()
        
        team_users: list[dict] = r.data or []  # type: ignore
        
        # Also include the admin themselves (in case they don't have created_by set)
        admin_record = [caller] if caller else []
        users = admin_record + team_users
        # Add user_id alias and attach permissions
        member_ids = [str(u.get("id")) for u in users if u.get("id")]
        perm_r = sb.table("user_permissions").select("*").in_("user_id", member_ids).execute() if member_ids else None
        perm_map: dict[str, dict] = {}  # type: ignore
        for p in ((perm_r.data or []) if perm_r else []):
            p_dict: dict = p  # type: ignore
            uid: str | None = p_dict.get("user_id")
            if uid:
                perm_map[uid] = p_dict
        
        for u in users:
            u_dict: dict = u  # type: ignore
            user_id_val = u_dict.get("id")
            u_dict["user_id"] = user_id_val
            if user_id_val:
                u_dict["permissions"] = perm_map.get(user_id_val, {})  # type: ignore
        
        logger.info(f"[GET /users/] Returning {len(users)} users")
        return {"users": users, "total": len(users)}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /users/] Error: {e}")
        raise HTTPException(500, str(e))


@router.post("/create-junior")
def create_user(body: CreateUserBody, authorization: str = Header(None)):
    """Create a new manager or junior user in the current admin's team."""
    caller = require_admin(authorization)

    if body.role not in ("manager", "junior"):
        raise HTTPException(400, "Admin can create only manager or junior users")

    sb = _get_supabase_admin()

    # 1. Create auth user
    try:
        auth_data: dict = {  # type: ignore
            "email":         body.email,
            "email_confirm": True,
            "user_metadata": {"full_name": body.full_name},
        }
        if body.password:
            auth_data["password"] = body.password
        
        auth_resp = sb.auth.admin.create_user(auth_data)  # type: ignore
        new_user_id = str(auth_resp.user.id)
    except Exception as e:
        msg = str(e).lower()
        if "already" in msg or "registered" in msg:
            raise HTTPException(400, "A user with this email already exists")
        raise HTTPException(400, f"Failed to create auth user: {str(e)}")

    # 2. Upsert users row (trigger may have already created it)
    try:
        sb.table("users").upsert({
            "id":         new_user_id,
            "email":      body.email,
            "full_name":  body.full_name,
            "role":       body.role,
            "is_active":  True,
            "created_by": caller["id"],
        }, on_conflict="id").execute()
    except Exception as e:
        raise HTTPException(500, f"Failed to save user profile: {str(e)}")

    # 3. Build permissions
    if body.role == "junior":
        # Junior: only explicitly granted permissions
        body_perms = body.permissions or {}
        perms = {k: bool(body_perms.get(k, False)) for k in ALL_PERMISSIONS}
    else:
        perms = _default_permissions_for_role(body.role)

    try:
        sb.table("user_permissions").upsert(
            {"user_id": new_user_id, **perms},
            on_conflict="user_id"
        ).execute()
    except Exception as e:
        raise HTTPException(500, f"Failed to save permissions: {str(e)}")

    # 4. Send password-reset email for magic link signup
    if not body.password:
        try:
            sb.auth.admin.generate_link({
                "type":  "recovery",
                "email": body.email,
            })
        except Exception:
            pass  # non-fatal

    return {
        "user_id":   new_user_id,
        "email":     body.email,
        "full_name": body.full_name,
        "role":      body.role,
        "created":   True,
    }


@router.put("/{user_id}/permissions")
def update_permissions(
    user_id: str,
    body: UpdatePermissionsBody,
    authorization: str = Header(None),
):
    """Update individual permission flags (admin only)."""
    caller = require_admin(authorization)

    # Validate keys
    invalid = [k for k in body.permissions if k not in ALL_PERMISSIONS]
    if invalid:
        raise HTTPException(400, f"Unknown permission keys: {invalid}")

    sb = _get_supabase_admin()
    try:
        target = _assert_admin_can_manage_target(sb, caller, user_id)
        if target.get("role") == "manager" and "manage_users" in body.permissions and body.permissions.get("manage_users"):
            raise HTTPException(400, "Manager users cannot be granted manage_users")
        if "manage_stores" in body.permissions and body.permissions.get("manage_stores"):
            raise HTTPException(400, "Only admin users can manage stores")

        r = sb.table("user_permissions").select("*").eq("user_id", user_id).execute()
        current = {}
        if r.data and len(r.data) > 0:
            current = r.data[0]  # type: ignore
        current_dict = current if isinstance(current, dict) else {k: False for k in ALL_PERMISSIONS}
        updated = {**current_dict, **{k: bool(v) for k, v in body.permissions.items()}}
        updated["user_id"] = user_id
        sb.table("user_permissions").upsert(updated, on_conflict="user_id").execute()
        return {"updated": True, "permissions": updated}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.put("/me/role")
def set_my_role(
    body: UpdateRoleBody,
    authorization: str = Header(None),
):
    """One-time onboarding role selection for self registration."""
    caller = get_current_user_full(authorization)
    
    # Allow role change if:
    # 1. User has no role (null/None)
    # 2. User has junior role (first-time setup)
    current_role = caller.get("role")
    if current_role and current_role not in ("junior", None):
        # User already has a non-junior role assigned - admin must change it
        raise HTTPException(403, "You already have a role assigned. Contact admin to change it.")
    
    if body.role not in ("admin", "manager", "junior"):
        raise HTTPException(400, "role must be admin, manager, or junior")

    sb = _get_supabase_admin()
    try:
        user_id = caller["id"]
        admin_owner_id = None
        if body.role in ("manager", "junior"):
            admin_email = (body.admin_email or "").strip().lower()
            if not admin_email:
                raise HTTPException(400, "admin_email is required for manager/junior onboarding")
            admin_lookup = sb.table("users").select("id, role, is_active") \
                .eq("email", admin_email).limit(1).execute()
            admin_row = admin_lookup.data[0] if admin_lookup.data else None
            if admin_row is not None and not isinstance(admin_row, dict):
                admin_row = None
            if not admin_row or admin_row.get("role") != "admin" or not admin_row.get("is_active", True):
                raise HTTPException(404, "Admin account not found for provided admin_email")
            admin_owner_id = admin_row.get("id")

        sb.table("users").update({
            "role": body.role,
            "created_by": admin_owner_id,
        }).eq("id", user_id).execute()

        perms = _default_permissions_for_role(body.role)
        sb.table("user_permissions").upsert({"user_id": user_id, **perms}, on_conflict="user_id").execute()
        
        logger.info(f"✅ User {user_id} set role to {body.role}")
        return {"updated": True, "role": body.role}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Error setting role: {e}")
        raise HTTPException(500, str(e))


@router.put("/{user_id}/role")
def update_role(
    user_id: str,
    body: UpdateRoleBody,
    authorization: str = Header(None),
):
    """Change another user's role (admin only)."""
    caller = require_admin(authorization)
    if body.role not in ("manager", "junior"):
        raise HTTPException(400, "Admin can assign only manager or junior roles to team users")

    sb = _get_supabase_admin()
    try:
        _assert_admin_can_manage_target(sb, caller, user_id)
        sb.table("users").update({"role": body.role}).eq("id", user_id).execute()
        perms = _default_permissions_for_role(body.role)
        sb.table("user_permissions").upsert({"user_id": user_id, **perms}, on_conflict="user_id").execute()
        
        return {"updated": True, "role": body.role}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.delete("/{user_id}")
def deactivate_user(user_id: str, authorization: str = Header(None)):
    """Deactivate (soft-delete) a user (admin only)."""
    caller = require_admin(authorization)

    sb = _get_supabase_admin()
    try:
        _assert_admin_can_manage_target(sb, caller, user_id)
        sb.table("users").update({"is_active": False}).eq("id", user_id).execute()
        # Also disable their Supabase Auth account
        try:
            sb.auth.admin.update_user_by_id(user_id, {"ban_duration": "876600h"})
        except Exception:
            pass  # best-effort
        return {"deactivated": True}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))


@router.post("/{user_id}/reactivate")
def reactivate_user(user_id: str, authorization: str = Header(None)):
    """Reactivate a deactivated user (admin only)."""
    caller = require_admin(authorization)
    sb = _get_supabase_admin()
    try:
        _assert_admin_can_manage_target(sb, caller, user_id)
        sb.table("users").update({"is_active": True}).eq("id", user_id).execute()
        try:
            sb.auth.admin.update_user_by_id(user_id, {"ban_duration": "none"})
        except Exception:
            pass
        return {"reactivated": True}
    except Exception as e:
        raise HTTPException(500, str(e))


@router.get("/{user_id}")
def get_user(user_id: str, authorization: str = Header(None)):
    """Get a single team user's profile + permissions (admin only)."""
    caller = require_admin(authorization)
    sb = _get_supabase_admin()
    try:
        user = _assert_admin_can_manage_target(sb, caller, user_id)
        user["user_id"] = user.get("id")
        
        p = sb.table("user_permissions").select("*").eq("user_id", user_id).execute()
        if p.data and len(p.data) > 0:
            perm_data = p.data[0]
            user["permissions"] = perm_data  # type: ignore
        else:
            user["permissions"] = {}
        return user
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))
