"""
User utilities for managing user data, roles, and permissions.

Provides functions for:
- Loading and saving user data from JSON
- CRUD operations on users
- Role and permission checking
"""

from pathlib import Path
import json
import uuid
from datetime import datetime
from typing import Optional, Dict, Any

logger_import = True

# ── Constants ─────────────────────────────────────────────────────────────────

ADMIN_ROLE = "admin"
MANAGER_ROLE = "manager"
JUNIOR_ROLE = "junior"

DEFAULT_PERMISSIONS = {
    "manage_products": False,
    "manage_collections": False,
    "manage_inventory": False,
    "manage_metafields": False,
    "manage_upload": False,
    "manage_export": False,
    "view_analytics": False,
    "manage_users": False,  # Only admins and managers
}

# Role-based default permissions
ROLE_PERMISSIONS = {
    ADMIN_ROLE: {
        "manage_products": True,
        "manage_collections": True,
        "manage_inventory": True,
        "manage_metafields": True,
        "manage_upload": True,
        "manage_export": True,
        "view_analytics": True,
        "manage_users": True,
    },
    MANAGER_ROLE: {
        "manage_products": True,
        "manage_collections": True,
        "manage_inventory": True,
        "manage_metafields": True,
        "manage_upload": True,
        "manage_export": True,
        "view_analytics": True,
        "manage_users": False,  # Managers can't create other managers
    },
    JUNIOR_ROLE: {
        "manage_products": False,  # Customizable per user
        "manage_collections": False,
        "manage_inventory": False,
        "manage_metafields": False,
        "manage_upload": False,
        "manage_export": False,
        "view_analytics": False,
        "manage_users": False,
    },
}

# ── File Paths ────────────────────────────────────────────────────────────────

DATA_DIR = Path(__file__).parent.parent / "data"
USERS_FILE = DATA_DIR / "users.json"


def _read_users_raw() -> dict:
    """Read raw users.json file."""
    if not USERS_FILE.exists():
        return {}
    try:
        data = json.loads(USERS_FILE.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _write_users_raw(data: dict):
    """Write users.json file."""
    DATA_DIR.mkdir(exist_ok=True)
    USERS_FILE.write_text(json.dumps(data, indent=2))


# ── Public API ────────────────────────────────────────────────────────────────

def load_users() -> dict:
    """Load all users from JSON."""
    return _read_users_raw()


def save_users(data: dict):
    """Save all users to JSON."""
    _write_users_raw(data)


def get_user(user_id: str) -> Optional[dict]:
    """Get a user by ID."""
    users = load_users()
    return users.get(user_id)


def user_exists(user_id: str) -> bool:
    """Check if a user exists."""
    return get_user(user_id) is not None


def create_user(
    email: str,
    full_name: str,
    role: str = JUNIOR_ROLE,
    permissions: Optional[dict] = None,
) -> str:
    """Create a new user and return the generated user_id."""
    users = load_users()

    # Generate unique user ID (use UUID-like format)
    user_id = f"user_{uuid.uuid4().hex[:12]}"

    # Default permissions based on role
    if permissions is None:
        permissions = ROLE_PERMISSIONS.get(role, DEFAULT_PERMISSIONS.copy())
    else:
        # Merge with role defaults
        role_perms = ROLE_PERMISSIONS.get(role, {})
        if isinstance(permissions, dict):
            merged = role_perms.copy()
            merged.update(permissions)
            permissions = merged
        else:
            permissions = role_perms

    users[user_id] = {
        "user_id": user_id,
        "email": email.lower(),
        "full_name": full_name,
        "role": role,
        "permissions": permissions,
        "is_active": True,
        "created_at": datetime.utcnow().isoformat() + "Z",
        "last_login": None,
    }

    save_users(users)
    return user_id


def update_user(user_id: str, updates: Any) -> Optional[dict]:
    """Update a user's information."""
    users = load_users()

    if user_id not in users:
        return None

    user = users[user_id]

    # Update fields
    if hasattr(updates, "full_name") and updates.full_name:
        user["full_name"] = updates.full_name

    if hasattr(updates, "role") and updates.role and updates.role in [ADMIN_ROLE, MANAGER_ROLE, JUNIOR_ROLE]:
        user["role"] = updates.role

    if hasattr(updates, "is_active") and updates.is_active is not None:
        user["is_active"] = updates.is_active

    if hasattr(updates, "permissions") and updates.permissions:
        # Merge permissions
        if isinstance(updates.permissions, dict):
            user["permissions"].update(updates.permissions)
        else:
            # Handle Pydantic model
            user["permissions"].update(updates.permissions.dict())

    users[user_id] = user
    save_users(users)
    return user


def delete_user(user_id: str) -> bool:
    """Deactivate a user (soft delete)."""
    users = load_users()

    if user_id not in users:
        return False

    users[user_id]["is_active"] = False
    save_users(users)
    return True


def get_user_role(user_id: str) -> Optional[str]:
    """Get user's role."""
    user = get_user(user_id)
    return user.get("role") if user else None


def get_user_permissions(user_id: str) -> dict:
    """Get user's permissions."""
    user = get_user(user_id)
    if not user:
        return {}
    return user.get("permissions", {})


def check_permission(user_id: str, permission: str) -> bool:
    """Check if user has a specific permission."""
    user = get_user(user_id)

    if not user or not user.get("is_active", True):
        return False

    role = user.get("role", JUNIOR_ROLE)

    # Admins have all permissions
    if role == ADMIN_ROLE:
        return True

    # Check explicit permission
    permissions = user.get("permissions", {})
    return permissions.get(permission, False)


def check_any_permission(user_id: str, *permissions: str) -> bool:
    """Check if user has any of the given permissions."""
    return any(check_permission(user_id, p) for p in permissions)


def check_all_permissions(user_id: str, *permissions: str) -> bool:
    """Check if user has all of the given permissions."""
    return all(check_permission(user_id, p) for p in permissions)


def initialize_admin_user(
    admin_user_id: str,
    email: str = "admin@example.com",
    full_name: str = "Admin User",
) -> str:
    """Initialize the first admin user (call once on app startup)."""
    if user_exists(admin_user_id):
        return admin_user_id

    return create_user(
        email=email,
        full_name=full_name,
        role=ADMIN_ROLE,
        permissions=ROLE_PERMISSIONS[ADMIN_ROLE],
    )


def get_users_by_role(role: str) -> list:
    """Get all users with a specific role."""
    users = load_users()
    return [
        user for user in users.values()
        if user.get("role") == role and user.get("is_active", True)
    ]


def get_users_with_permission(permission: str) -> list:
    """Get all users with a specific permission."""
    users = load_users()
    return [
        user for user in users.values()
        if user.get("permissions", {}).get(permission, False) and user.get("is_active", True)
    ]
