"""
User management and permission system.

Endpoints for creating, reading, updating, and deleting users with role-based permissions.
Supports admin users creating junior users with granular permission controls.
"""

from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel, EmailStr
from typing import Optional, List
import logging
import json
from datetime import datetime
from pathlib import Path

from .auth_utils import require_authenticated_user
from .user_utils import (
    load_users,
    save_users,
    get_user,
    create_user,
    update_user,
    delete_user,
    get_user_role,
    check_permission,
    ADMIN_ROLE,
    MANAGER_ROLE,
    JUNIOR_ROLE,
)

logger = logging.getLogger(__name__)
router = APIRouter()


class UserPermissions(BaseModel):
    manage_products: bool = False
    manage_collections: bool = False
    manage_inventory: bool = False
    manage_metafields: bool = False
    manage_upload: bool = False
    manage_export: bool = False
    view_analytics: bool = False


class CreateJuniorUserRequest(BaseModel):
    email: str
    full_name: str
    role: str = JUNIOR_ROLE  # "junior", "manager"
    permissions: Optional[UserPermissions] = None
    password: str = ""  # For backend-only user creation


class UpdateUserRequest(BaseModel):
    full_name: Optional[str] = None
    role: Optional[str] = None
    permissions: Optional[UserPermissions] = None
    is_active: Optional[bool] = None


class UserResponse(BaseModel):
    user_id: str
    email: str
    full_name: str
    role: str
    permissions: UserPermissions
    is_active: bool
    created_at: str
    last_login: Optional[str] = None


# ── Routes ────────────────────────────────────────────────────────────────────

@router.get("/")
def list_users(
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """List all users (admin/manager only)."""
    # Check if current user has permission
    current_user = get_user(current_user_id)
    if not current_user:
        raise HTTPException(status_code=401, detail="User not found")

    if not check_permission(current_user_id, "manage_users"):
        raise HTTPException(status_code=403, detail="Permission denied. Only admins can manage users.")

    users = load_users()
    users_list = []

    for user_id, user_data in users.items():
        if user_id == "__legacy__":
            continue
        users_list.append({
            "user_id": user_id,
            "email": user_data.get("email", ""),
            "full_name": user_data.get("full_name", ""),
            "role": user_data.get("role", JUNIOR_ROLE),
            "is_active": user_data.get("is_active", True),
            "created_at": user_data.get("created_at"),
            "last_login": user_data.get("last_login"),
            "permissions": user_data.get("permissions", {}),
        })

    return {
        "success": True,
        "users": users_list,
        "count": len(users_list),
    }


@router.post("/create-junior")
def create_junior_user(
    req: CreateJuniorUserRequest,
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """Create a new junior user (admin/manager only)."""
    current_user = get_user(current_user_id)
    if not current_user:
        raise HTTPException(status_code=401, detail="User not found")

    if not check_permission(current_user_id, "manage_users"):
        raise HTTPException(status_code=403, detail="Permission denied. Only admins can create users.")

    # Check if user already exists
    users = load_users()
    if any(u.get("email") == req.email.lower() for u in users.values() if u):
        raise HTTPException(status_code=400, detail="User with this email already exists")

    # Create the user
    user_id = create_user(
        email=req.email.lower(),
        full_name=req.full_name,
        role=req.role,
        permissions=req.permissions or UserPermissions(),
    )

    new_user = get_user(user_id)
    return {
        "success": True,
        "message": f"Junior user '{req.email}' created successfully",
        "user": {
            "user_id": user_id,
            "email": new_user.get("email"),
            "full_name": new_user.get("full_name"),
            "role": new_user.get("role"),
            "permissions": new_user.get("permissions"),
            "created_at": new_user.get("created_at"),
        },
    }


@router.get("/{user_id}")
def get_user_info(
    user_id: str,
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """Get user info (admin/manager or self)."""
    # Allow users to view their own info, or admins to view any user
    if user_id != current_user_id:
        if not check_permission(current_user_id, "manage_users"):
            raise HTTPException(status_code=403, detail="Permission denied")

    user = get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "success": True,
        "user": {
            "user_id": user_id,
            "email": user.get("email"),
            "full_name": user.get("full_name"),
            "role": user.get("role"),
            "permissions": user.get("permissions", {}),
            "is_active": user.get("is_active", True),
            "created_at": user.get("created_at"),
            "last_login": user.get("last_login"),
        },
    }


@router.put("/{user_id}")
def update_user_info(
    user_id: str,
    req: UpdateUserRequest,
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """Update user info (admin/manager or self, with limitations)."""
    user = get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    # Check permissions
    is_self = user_id == current_user_id
    is_admin = check_permission(current_user_id, "manage_users")

    if not is_self and not is_admin:
        raise HTTPException(status_code=403, detail="Permission denied")

    # Users can't change their own role
    if is_self and req.role and req.role != user.get("role"):
        raise HTTPException(status_code=403, detail="Cannot change your own role")

    # Users can't change their own permissions
    if is_self and req.permissions:
        raise HTTPException(status_code=403, detail="Cannot change your own permissions")

    # Update the user
    updated_user = update_user(user_id, req)

    return {
        "success": True,
        "message": "User updated successfully",
        "user": {
            "user_id": user_id,
            "email": updated_user.get("email"),
            "full_name": updated_user.get("full_name"),
            "role": updated_user.get("role"),
            "permissions": updated_user.get("permissions"),
            "is_active": updated_user.get("is_active"),
        },
    }


@router.delete("/{user_id}")
def deactivate_user(
    user_id: str,
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """Deactivate a user (admin only)."""
    if not check_permission(current_user_id, "manage_users"):
        raise HTTPException(status_code=403, detail="Permission denied. Only admins can delete users.")

    if user_id == current_user_id:
        raise HTTPException(status_code=400, detail="Cannot deactivate your own account")

    user = get_user(user_id)
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    delete_user(user_id)

    return {
        "success": True,
        "message": f"User {user.get('email')} has been deactivated",
    }


@router.post("/update-last-login/{user_id}")
def log_last_login(
    user_id: str,
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """Update last login timestamp (internal, self only)."""
    if user_id != current_user_id:
        raise HTTPException(status_code=403, detail="Can only update own last login")

    users = load_users()
    if user_id not in users:
        users[user_id] = {}

    users[user_id]["last_login"] = datetime.utcnow().isoformat() + "Z"
    save_users(users)

    return {"success": True}


@router.get("/me/permissions")
def get_current_user_permissions(
    current_user_id: str = Depends(require_authenticated_user),
) -> dict:
    """Get current user's permissions."""
    user = get_user(current_user_id)
    if not user:
        raise HTTPException(status_code=401, detail="User not found")

    return {
        "success": True,
        "user_id": current_user_id,
        "role": user.get("role", JUNIOR_ROLE),
        "permissions": user.get("permissions", {}),
    }
