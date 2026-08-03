/**
 * Permission helper functions - DEPRECATED: Use useAuth hook instead
 * Kept for backward compatibility
 */

export function canManageProducts() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_products === true;
  } catch {
    return false;
  }
}

export function canDeleteProducts() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.delete_products === true;
  } catch {
    return false;
  }
}

export function canManageCollections() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_collections === true;
  } catch {
    return false;
  }
}

export function canManageInventory() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_inventory === true;
  } catch {
    return false;
  }
}

export function canManageMetafields() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_metafields === true;
  } catch {
    return false;
  }
}

export function canUpload() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_upload === true;
  } catch {
    return false;
  }
}

export function canExport() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_export === true;
  } catch {
    return false;
  }
}

export function canUseAI() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.use_ai === true;
  } catch {
    return false;
  }
}

export function canManageStores() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_stores === true;
  } catch {
    return false;
  }
}

export function canManageUsers() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.manage_users === true;
  } catch {
    return false;
  }
}

export function canViewAnalytics() {
  try {
    const perms = JSON.parse(localStorage.getItem("user_permissions") || "{}");
    return perms.view_analytics === true;
  } catch {
    return false;
  }
}

export function isUserLoggedInWithRole() {
  try {
    const role = localStorage.getItem("user_role");
    return role !== null && role !== undefined;
  } catch {
    return false;
  }
}

export function getUserRole() {
  try {
    return localStorage.getItem("user_role") || null;
  } catch {
    return null;
  }
}

export function setUserPermissions(permissions) {
  try {
    localStorage.setItem("user_permissions", JSON.stringify(permissions));
  } catch {
    console.error("Failed to set user permissions");
  }
}

export function setUserRole(role) {
  try {
    localStorage.setItem("user_role", role);
  } catch {
    console.error("Failed to set user role");
  }
}

export function clearUserPermissions() {
  try {
    localStorage.removeItem("user_permissions");
    localStorage.removeItem("user_role");
  } catch {
    console.error("Failed to clear user permissions");
  }
}
