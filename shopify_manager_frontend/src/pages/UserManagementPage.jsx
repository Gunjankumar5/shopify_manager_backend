import { useState, useEffect, useRef } from "react";
import { supabase } from "../lib/supabaseClient";
import { useAuth } from "../hooks/useAuth";
import { API_BASE_URL } from "../api/config";


// ── Permission definitions ────────────────────────────────────────────────────
const ALL_PERMISSIONS = [
  { key: "manage_products",    label: "Manage Products",    icon: "package",   desc: "Create & edit products" },
  { key: "delete_products",    label: "Delete Products",    icon: "trash",     desc: "Permanently delete products" },
  { key: "manage_collections", label: "Collections",        icon: "tag",       desc: "Create & edit collections" },
  { key: "manage_inventory",   label: "Inventory",          icon: "layers",    desc: "Update stock levels" },
  { key: "manage_metafields",  label: "Metafields",         icon: "code",      desc: "Edit metafield values" },
  { key: "manage_upload",      label: "Bulk Upload",        icon: "upload",    desc: "Upload CSV/Excel files" },
  { key: "manage_export",      label: "Export",             icon: "download",  desc: "Export product data" },
  { key: "use_ai",             label: "AI Features",        icon: "zap",       desc: "Use AI content generation" },
  { key: "manage_stores",      label: "Manage Stores",      icon: "link",      desc: "Connect/disconnect stores" },
  { key: "manage_users",       label: "Manage Users",       icon: "users",     desc: "Create & manage team members" },
  { key: "view_analytics",     label: "Analytics",          icon: "bar-chart", desc: "View analytics dashboard" },
];

const ROLES = {
  admin:   { label: "Admin",   color: "#ef4444", bg: "rgba(239,68,68,0.12)",   desc: "Full access to everything" },
  manager: { label: "Manager", color: "#f59e0b", bg: "rgba(245,158,11,0.12)",  desc: "All except user management" },
  junior:  { label: "Junior",  color: "#6366f1", bg: "rgba(99,102,241,0.12)",  desc: "Only granted permissions" },
};

const MANAGEABLE_ROLES = ["manager", "junior"];

// ── Auth helper ───────────────────────────────────────────────────────────────
async function authHeaders() {
  if (!supabase) return {};
  const { data } = await supabase.auth.getSession();
  const token = data?.session?.access_token;
  console.log("[authHeaders] Token:", token ? `${token.substring(0,20)}...` : "NO TOKEN");
  return token ? { Authorization: `Bearer ${token}` } : {};
}

async function apiFetch(path, options = {}) {
  const headers = {
    "Content-Type": "application/json",
    ...(await authHeaders()),
    ...(options.headers || {}),
  };
  console.log("[apiFetch] Headers:", { Authorization: headers.Authorization ? "Bearer..." : "MISSING" });
  const res = await fetch(`${API_BASE_URL}${path}`, { ...options, headers });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.detail || json.error || `HTTP ${res.status}`);
  return json;
}

// ── Main Page Component ───────────────────────────────────────────────────────
export default function UserManagementPage({ toast }) {
  const { user, role, can, signOut, loading: authLoading } = useAuth();
  const [users, setUsers] = useState([]);
  const [selectedUser, setSelectedUser] = useState(null);
  const [loading, setLoading] = useState(true);
  const [updating, setUpdating] = useState(false);
  const [error, setError] = useState("");
  const [createForm, setCreateForm] = useState({
    full_name: "",
    email: "",
    role: "manager",
    password: "",
  });
  const [creating, setCreating] = useState(false);
  const fetchedRef = useRef(false);

  const c = {
    bg: "var(--bg-primary)",
    card: "var(--bg-card)",
    border: "var(--border-subtle)",
    text: "var(--text-primary)",
    muted: "var(--text-muted)",
    accent: "var(--accent)",
    accentLight: "rgba(99,102,241,0.14)",
    danger: "var(--danger-text)",
    dangerBg: "var(--danger-light)",
    dangerBorder: "var(--danger-border)",
    success: "var(--success-text)",
    successBg: "var(--success-light)",
  };

  const activeCount = users.filter((u) => u.is_active).length;
  const managerCount = users.filter((u) => u.role === "manager").length;
  const juniorCount = users.filter((u) => u.role === "junior").length;

  // Fetch users list
  useEffect(() => {
    // Wait for auth to finish loading before attempting to fetch
    if (authLoading) return;
    
    // Guard to prevent duplicate fetches (React Strict Mode causes unmount/remount in dev)
    if (fetchedRef.current) return;
    fetchedRef.current = true;

    const fetchUsers = async () => {
      try {
        setLoading(true);
        setError("");
        console.log("[UserManagementPage] Starting to fetch users...");
        console.log("[UserManagementPage] Auth loaded - role:", role);
        console.log("[UserManagementPage] can('manage_users'):", can("manage_users"));
        
        if (!can("manage_users")) {
          console.log("[UserManagementPage] User does not have manage_users permission");
          setError("You don't have permission to manage users");
          setLoading(false);
          return;
        }

        const data = await apiFetch("/users/");
        console.log("[UserManagementPage] Users fetched:", data);
        
        setUsers(data.users || []);
        if (data.users?.length > 0) {
          setSelectedUser(data.users[0]);
        }
      } catch (err) {
        console.error("[UserManagementPage] Fetch error:", err);
        setError(err.message || "Failed to fetch users");
      } finally {
        setLoading(false);
      }
    };

    fetchUsers();
  }, [authLoading, can]);

  // Update user role
  const handleRoleChange = async (userId, newRole) => {
    if (!selectedUser || !can("manage_users")) return;
    try {
      setUpdating(true);
      setError("");
      await apiFetch(`/users/${userId}/role`, {
        method: "PUT",
        body: JSON.stringify({ role: newRole }),
      });
      // Refresh user
      setSelectedUser((prev) => ({ ...prev, role: newRole }));
      // Refresh list
      const data = await apiFetch("/users/");
      setUsers(data.users || []);
      toast?.("Role updated successfully", "success");
    } catch (err) {
      setError(err.message);
      toast?.(err.message, "error");
    } finally {
      setUpdating(false);
    }
  };

  // Update user permission
  const handlePermissionToggle = async (userId, permission, value) => {
    if (!selectedUser || !can("manage_users")) return;
    try {
      setUpdating(true);
      setError("");
      await apiFetch(`/users/${userId}/permissions`, {
        method: "PUT",
        body: JSON.stringify({ permissions: { [permission]: value } }),
      });
      // Update selected user
      setSelectedUser((prev) => ({
        ...prev,
        permissions: { ...prev.permissions, [permission]: value },
      }));
      // Refresh list
      const data = await apiFetch("/users/");
      setUsers(data.users || []);
      toast?.(
        `Permission ${value ? "granted" : "revoked"}`,
        "success"
      );
    } catch (err) {
      setError(err.message);
      toast?.(err.message, "error");
    } finally {
      setUpdating(false);
    }
  };

  // Toggle all permissions
  const handleToggleAllPermissions = async (userId, enable) => {
    if (!selectedUser || !can("manage_users")) return;
    try {
      setUpdating(true);
      setError("");
      const permissions = {};
      ALL_PERMISSIONS.forEach((p) => {
        permissions[p.key] = enable;
      });
      await apiFetch(`/users/${userId}/permissions`, {
        method: "PUT",
        body: JSON.stringify({ permissions }),
      });
      // Update selected user
      setSelectedUser((prev) => ({
        ...prev,
        permissions,
      }));
      // Refresh list
      const data = await apiFetch("/users/");
      setUsers(data.users || []);
      toast?.(
        enable ? "All permissions granted" : "All permissions revoked",
        "success"
      );
    } catch (err) {
      setError(err.message);
      toast?.(err.message, "error");
    } finally {
      setUpdating(false);
    }
  };

  // Toggle user active status
  const handleToggleActive = async (userId, isActive) => {
    if (!selectedUser || !can("manage_users")) return;
    try {
      setUpdating(true);
      setError("");
      // Deactivate or reactivate using appropriate endpoint
      const endpoint = isActive ? `/users/${userId}` : `/users/${userId}/reactivate`;
      const method = isActive ? "DELETE" : "POST";
      await apiFetch(endpoint, { method, body: "" });
      // Update selected user
      setSelectedUser((prev) => ({
        ...prev,
        is_active: !isActive,
      }));
      // Refresh list
      const data = await apiFetch("/users/");
      setUsers(data.users || []);
      toast?.(
        (isActive ? "User deactivated" : "User activated"),
        "success"
      );
    } catch (err) {
      setError(err.message);
      toast?.(err.message, "error");
    } finally {
      setUpdating(false);
    }
  };

  const handleCreateUser = async () => {
    if (!createForm.full_name.trim() || !createForm.email.trim()) {
      toast?.("Name and email are required", "error");
      return;
    }
    if (!MANAGEABLE_ROLES.includes(createForm.role)) {
      toast?.("Only manager or junior can be created", "error");
      return;
    }

    try {
      setCreating(true);
      setError("");
      await apiFetch("/users/create-junior", {
        method: "POST",
        body: JSON.stringify({
          full_name: createForm.full_name.trim(),
          email: createForm.email.trim().toLowerCase(),
          role: createForm.role,
          password: createForm.password.trim() || null,
          permissions: {},
        }),
      });
      setCreateForm({
        full_name: "",
        email: "",
        role: "manager",
        password: "",
      });
      const data = await apiFetch("/users/");
      setUsers(data.users || []);
      toast?.("Team member created successfully", "success");
    } catch (err) {
      setError(err.message || "Failed to create user");
      toast?.(err.message || "Failed to create user", "error");
    } finally {
      setCreating(false);
    }
  };

  const selectedIsSelf = selectedUser?.id === user?.id;
  const selectedIsAdmin = selectedUser?.role === "admin";
  const disableAdminOps = updating || selectedIsSelf || selectedIsAdmin;

  if (loading) {
    return (
      <div style={{ minHeight: "100vh", background: c.bg, display: "grid", placeItems: "center" }}>
        <div style={{ textAlign: "center" }}>
          {error ? (
            <>
              <p style={{ color: c.danger, fontWeight: 600, marginBottom: "12px" }}>❌ {error}</p>
              <p style={{ color: c.muted, fontSize: "14px" }}>Check the console for more details</p>
            </>
          ) : (
            <p style={{ color: c.muted }}>Loading...</p>
          )}
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ minHeight: "100vh", background: c.bg, display: "grid", placeItems: "center" }}>
        <div style={{ textAlign: "center" }}>
          <p style={{ color: c.danger, fontWeight: 600, marginBottom: "12px" }}>❌ {error}</p>
          <p style={{ color: c.muted, fontSize: "14px" }}>Check the console for more details</p>
        </div>
      </div>
    );
  }

  return (
    <div
      style={{
        minHeight: "100vh",
        padding: "24px",
        background:
          "radial-gradient(circle at top left, rgba(99,102,241,0.20), transparent 28%), radial-gradient(circle at bottom right, rgba(16,185,129,0.12), transparent 30%), var(--bg-primary)",
      }}
    >
      <div style={{ maxWidth: "1600px", margin: "0 auto", display: "grid", gap: "20px" }}>
        <div
          style={{
            display: "flex",
            justifyContent: "space-between",
            alignItems: "flex-end",
            gap: "16px",
            flexWrap: "wrap",
          }}
        >
          <div>
            <div
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
                padding: "6px 12px",
                borderRadius: 999,
                background: "rgba(99,102,241,0.12)",
                border: "1px solid rgba(99,102,241,0.18)",
                color: "#c7d2fe",
                fontSize: 11,
                fontWeight: 700,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                marginBottom: 10,
              }}
            >
              Team access management
            </div>
            <h1
              style={{
                fontSize: "clamp(24px, 3vw, 34px)",
                fontWeight: 800,
                color: c.text,
                margin: 0,
                letterSpacing: "-0.04em",
                fontFamily: "var(--font-display)",
              }}
            >
              User Management
            </h1>
            <p style={{ marginTop: 8, color: c.muted, fontSize: 14, maxWidth: 760, lineHeight: 1.6 }}>
              Create managers and juniors, adjust permissions, and keep every admin team isolated from the others.
            </p>
          </div>
          <div style={{ display: "flex", gap: 10, flexWrap: "wrap" }}>
            {[
              { label: "Visible", value: users.length },
              { label: "Active", value: activeCount },
              { label: "Managers", value: managerCount },
              { label: "Juniors", value: juniorCount },
            ].map((item) => (
              <div
                key={item.label}
                style={{
                  minWidth: 110,
                  padding: "12px 14px",
                  borderRadius: 18,
                  background: "rgba(255,255,255,0.04)",
                  border: `1px solid ${c.border}`,
                  boxShadow: "0 12px 30px rgba(0,0,0,0.18)",
                }}
              >
                <div style={{ color: c.muted, fontSize: 11, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                  {item.label}
                </div>
                <div style={{ color: c.text, fontSize: 20, fontWeight: 800, marginTop: 4, fontFamily: "var(--font-display)" }}>
                  {item.value}
                </div>
              </div>
            ))}
          </div>
        </div>

        <div style={{ display: "grid", gridTemplateColumns: "minmax(320px, 360px) minmax(0, 1fr)", gap: 20, minHeight: "calc(100vh - 160px)" }}>
      {/* ── LEFT PANEL (User List) ── */}
      <div
        style={{
          width: "100%",
          borderRight: `1px solid ${c.border}`,
          overflow: "hidden",
          borderRadius: 28,
          background: "linear-gradient(180deg, rgba(23,26,38,0.96), rgba(14,16,24,0.98))",
          boxShadow: "0 24px 80px rgba(0,0,0,0.28)",
          border: `1px solid ${c.border}`,
        }}
      >
        <div style={{ padding: "20px", borderBottom: `1px solid ${c.border}`, background: "rgba(255,255,255,0.02)" }}>
          <h2 style={{ fontSize: "16px", fontWeight: 800, color: c.text, margin: "0 0 8px 0", fontFamily: "var(--font-display)" }}>
            Team Members ({users.length})
          </h2>
          <p style={{ margin: 0, color: c.muted, fontSize: 12, lineHeight: 1.5 }}>
            Create and manage only the users in your own admin team.
          </p>
          <form
            style={{ display: "grid", gap: "10px", marginTop: 16 }}
            onSubmit={(e) => {
              e.preventDefault();
              handleCreateUser();
            }}
          >
            <input
              value={createForm.full_name}
              onChange={(e) => setCreateForm((p) => ({ ...p, full_name: e.target.value }))}
              placeholder="Full name"
              style={{
                padding: "11px 12px",
                borderRadius: "12px",
                border: `1px solid ${c.border}`,
                background: c.card,
                color: c.text,
                fontSize: "13px",
                outline: "none",
              }}
            />
            <input
              value={createForm.email}
              onChange={(e) => setCreateForm((p) => ({ ...p, email: e.target.value }))}
              placeholder="Email"
              style={{
                padding: "11px 12px",
                borderRadius: "12px",
                border: `1px solid ${c.border}`,
                background: c.card,
                color: c.text,
                fontSize: "13px",
                outline: "none",
              }}
            />
            <select
              value={createForm.role}
              onChange={(e) => setCreateForm((p) => ({ ...p, role: e.target.value }))}
              style={{
                padding: "11px 12px",
                borderRadius: "12px",
                border: `1px solid ${c.border}`,
                background: c.card,
                color: c.text,
                fontSize: "13px",
                outline: "none",
                cursor: "pointer",
              }}
            >
              <option value="manager">Manager</option>
              <option value="junior">Junior</option>
            </select>
            <input
              type="password"
              value={createForm.password}
              onChange={(e) => setCreateForm((p) => ({ ...p, password: e.target.value }))}
              placeholder="Password (optional)"
              autoComplete="new-password"
              style={{
                padding: "11px 12px",
                borderRadius: "12px",
                border: `1px solid ${c.border}`,
                background: c.card,
                color: c.text,
                fontSize: "13px",
                outline: "none",
              }}
            />
            <button
              type="submit"
              disabled={creating}
              style={{
                padding: "11px 14px",
                borderRadius: "12px",
                border: "none",
                background: "var(--accent-gradient)",
                color: "#fff",
                fontWeight: 700,
                fontSize: "13px",
                cursor: creating ? "not-allowed" : "pointer",
                opacity: creating ? 0.7 : 1,
                boxShadow: "0 14px 28px rgba(99,102,241,0.24)",
              }}
            >
              {creating ? "Creating..." : "Create Team User"}
            </button>
          </form>
        </div>

        <div style={{ display: "flex", flexDirection: "column", padding: 10, gap: 10 }}>
          {users.map((u) => (
            <button
              key={u.id}
              onClick={() => setSelectedUser(u)}
              style={{
                padding: "14px 16px",
                border: `1px solid ${selectedUser?.id === u.id ? "rgba(99,102,241,0.30)" : c.border}`,
                background: selectedUser?.id === u.id ? "rgba(99,102,241,0.14)" : "rgba(255,255,255,0.02)",
                borderRadius: 18,
                cursor: "pointer",
                textAlign: "left",
                transition: "transform 0.2s, background 0.2s, border-color 0.2s",
                boxShadow: selectedUser?.id === u.id ? "0 12px 30px rgba(99,102,241,0.12)" : "none",
              }}
              onMouseEnter={(e) => {
                if (selectedUser?.id !== u.id) {
                  e.currentTarget.style.background = "rgba(255,255,255,0.05)";
                  e.currentTarget.style.transform = "translateY(-1px)";
                }
              }}
              onMouseLeave={(e) => {
                if (selectedUser?.id !== u.id) {
                  e.currentTarget.style.background = "rgba(255,255,255,0.02)";
                  e.currentTarget.style.transform = "translateY(0)";
                }
              }}
            >
              <div style={{ fontSize: "13px", fontWeight: 600, color: c.text }}>
                {u.full_name || u.email}
              </div>
              <div
                style={{
                  fontSize: "11px",
                  color: c.muted,
                  marginTop: "4px",
                }}
              >
                {u.email}
              </div>
              <div style={{ fontSize: "10px", color: c.muted, marginTop: "4px" }}>
                {u.is_active ? "✓ Active" : "✗ Inactive"}
              </div>
            </button>
          ))}
        </div>
      </div>

      {/* ── RIGHT PANEL (User Details) ── */}
      <div style={{ flex: 1, overflow: "auto", display: "flex", flexDirection: "column", borderRadius: 28, background: "linear-gradient(180deg, rgba(23,26,38,0.76), rgba(13,15,22,0.94))", boxShadow: "0 24px 80px rgba(0,0,0,0.28)", border: `1px solid ${c.border}` }}>
        {selectedUser ? (
          <>
            {/* Header */}
            <div style={{ padding: "22px", borderBottom: `1px solid ${c.border}`, background: "rgba(255,255,255,0.02)" }}>
              <div style={{ display: "flex", justifyContent: "space-between", alignItems: "start", marginBottom: "16px", gap: 16, flexWrap: "wrap" }}>
                <div>
                  <h1 style={{ fontSize: "22px", fontWeight: 800, color: c.text, margin: 0, marginBottom: "6px", fontFamily: "var(--font-display)" }}>
                    {selectedUser.full_name}
                  </h1>
                  <div style={{ fontSize: "13px", color: c.muted }}>
                    {selectedUser.email}
                  </div>
                </div>
                <div style={{ display: "flex", gap: "8px" }}>
                  <button
                    onClick={() => handleToggleActive(selectedUser.id, selectedUser.is_active)}
                    disabled={disableAdminOps}
                    style={{
                      padding: "8px 16px",
                      borderRadius: "999px",
                      background: selectedUser.is_active ? "rgba(255,255,255,0.03)" : c.dangerBg,
                      border: `1px solid ${selectedUser.is_active ? c.border : c.dangerBorder}`,
                      color: selectedUser.is_active ? c.muted : c.danger,
                      cursor: disableAdminOps ? "not-allowed" : "pointer",
                      fontSize: "12px",
                      fontWeight: 600,
                      opacity: disableAdminOps ? 0.6 : 1,
                      transition: "all 0.2s",
                    }}
                  >
                    {selectedUser.is_active ? "Deactivate" : "Activate"}
                  </button>
                </div>
              </div>

              {error && (
                <div style={{
                  padding: "12px 14px",
                  background: c.dangerBg,
                  border: `1px solid ${c.dangerBorder}`,
                  borderRadius: "14px",
                  color: c.danger,
                  fontSize: "12px",
                }}>
                  {error}
                </div>
              )}
            </div>

            {/* Content */}
            <div style={{ flex: 1, padding: "24px", overflow: "auto", display: "grid", gap: 28 }}>
              {/* Role Section */}
              <div style={{ padding: 20, borderRadius: 24, border: `1px solid ${c.border}`, background: "rgba(255,255,255,0.03)" }}>
                <h2 style={{ fontSize: "14px", fontWeight: 800, color: c.text, margin: "0 0 16px 0", textTransform: "uppercase", letterSpacing: "0.08em" }}>
                  Role
                </h2>
                <div style={{ display: "flex", flexDirection: "column", gap: "12px" }}>
                  {MANAGEABLE_ROLES.map((roleKey) => {
                    const roleInfo = ROLES[roleKey];
                    return (
                    <label
                      key={roleKey}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        padding: "14px",
                        background: selectedUser.role === roleKey ? roleInfo.bg : c.card,
                        border: `1px solid ${c.border}`,
                        borderRadius: "16px",
                        cursor: disableAdminOps ? "not-allowed" : "pointer",
                        transition: "all 0.2s",
                        opacity: disableAdminOps ? 0.6 : 1,
                      }}
                    >
                      <input
                        type="radio"
                        name="role"
                        value={roleKey}
                        checked={selectedUser.role === roleKey}
                        onChange={() => handleRoleChange(selectedUser.id, roleKey)}
                        disabled={disableAdminOps}
                        style={{ marginRight: "12px", cursor: "pointer" }}
                      />
                      <div>
                        <div style={{ fontSize: "13px", fontWeight: 600, color: roleInfo.color }}>
                          {roleInfo.label}
                        </div>
                        <div style={{ fontSize: "12px", color: c.muted, marginTop: "2px" }}>
                          {roleInfo.desc}
                        </div>
                      </div>
                    </label>
                  );
                  })}
                </div>
              </div>

              {/* Permissions Section */}
              <div style={{ padding: 20, borderRadius: 24, border: `1px solid ${c.border}`, background: "rgba(255,255,255,0.03)" }}>
                <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "16px", gap: 12, flexWrap: "wrap" }}>
                  <h2 style={{ fontSize: "14px", fontWeight: 800, color: c.text, margin: 0, textTransform: "uppercase", letterSpacing: "0.08em" }}>
                    Permissions
                  </h2>
                  <div style={{ display: "flex", gap: "8px" }}>
                    <button
                      onClick={() => handleToggleAllPermissions(selectedUser.id, true)}
                      disabled={updating}
                      style={{
                        padding: "6px 12px",
                        background: "rgba(255,255,255,0.03)",
                        border: `1px solid ${c.border}`,
                        color: c.text,
                        borderRadius: "999px",
                        cursor: updating ? "not-allowed" : "pointer",
                        fontSize: "11px",
                        fontWeight: 600,
                        opacity: updating ? 0.6 : 1,
                        transition: "all 0.2s",
                      }}
                    >
                      Grant all
                    </button>
                    <button
                      onClick={() => handleToggleAllPermissions(selectedUser.id, false)}
                      disabled={updating}
                      style={{
                        padding: "6px 12px",
                        background: "transparent",
                        border: `1px solid ${c.border}`,
                        color: c.text,
                        borderRadius: "6px",
                        cursor: updating ? "not-allowed" : "pointer",
                        fontSize: "11px",
                        fontWeight: 600,
                        opacity: updating ? 0.6 : 1,
                        transition: "all 0.2s",
                      }}
                    >
                      Revoke all
                    </button>
                  </div>
                </div>

                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: "12px" }}>
                  {ALL_PERMISSIONS.map((perm) => (
                    <label
                      key={perm.key}
                      style={{
                        display: "flex",
                        alignItems: "flex-start",
                        padding: "14px",
                        background: c.card,
                        border: `1px solid ${c.border}`,
                        borderRadius: "16px",
                        cursor: updating ? "not-allowed" : "pointer",
                        transition: "all 0.2s",
                        opacity: updating ? 0.6 : 1,
                        minHeight: 92,
                      }}
                    >
                      <input
                        type="checkbox"
                        checked={selectedUser.permissions?.[perm.key] || false}
                        onChange={(e) =>
                          handlePermissionToggle(selectedUser.id, perm.key, e.target.checked)
                        }
                        disabled={disableAdminOps}
                        style={{ marginRight: "10px", marginTop: "2px", cursor: "pointer" }}
                      />
                      <div>
                        <div style={{ fontSize: "13px", fontWeight: 600, color: c.text }}>
                          {perm.label}
                        </div>
                        <div style={{ fontSize: "11px", color: c.muted, marginTop: "2px" }}>
                          {perm.desc}
                        </div>
                      </div>
                    </label>
                  ))}
                </div>
              </div>
            </div>
          </>
        ) : (
          <div style={{ flex: 1, display: "grid", placeItems: "center", color: c.muted }}>
            Select a user to manage permissions
          </div>
        )}
        </div>
      </div>
    </div>
  </div>
  );
}
