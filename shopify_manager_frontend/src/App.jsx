import { useEffect, useMemo, useState, lazy, Suspense } from "react";
import GlobalStyles from "./components/GlobalStyles";
import { useToast, Toasts } from "./components/Toast";
import Sidebar from "./components/Sidebar";
import { Ico } from "./components/Icons";
import { hasSupabaseConfig, supabase } from "./lib/supabaseClient";
import { useAuth } from "./hooks/useAuth";
import AccessDenied from "./components/AccessDenied";

// ── Lazy-load pages for better code splitting ────────────────────────────────
const ProductsPage = lazy(() => import("./pages/ProductsPage"));
const UploadPage = lazy(() => import("./pages/UploadPage"));
const CollectionsPage = lazy(() => import("./pages/CollectionsPage"));
const InventoryPage = lazy(() => import("./pages/InventoryPage"));
const ConnectStore = lazy(() => import("./pages/ConnectStore"));
const ExportPage = lazy(() => import("./pages/ExportPage"));
const AuthPage = lazy(() => import("./pages/AuthPage"));
const UserManagementPage = lazy(() => import("./pages/UserManagementPage"));
const MetafieldsPage = lazy(() => import("./pages/MetafieldsPage"));

// Loading fallback component
const PageLoader = () => (
  <div style={{
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    height: "100vh",
    background: "var(--bg-primary)",
    color: "var(--text-muted)",
  }}>
    <div>Loading...</div>
  </div>
);

export default function App() {
  const { user, role, loading, can, signOut } = useAuth();
  const [page, setPage] = useState(() => {
    try {
      return localStorage.getItem("currentPage") || "products";
    } catch {
      return "products";
    }
  });
  const [isMobile, setIsMobile] = useState(() => window.innerWidth <= 1024);
  const [isSidebarOpen, setIsSidebarOpen] = useState(false);
  const [activeStore, setActiveStore] = useState(null);
  const [pageKey, setPageKey] = useState(0);
  const { toasts, add, remove } = useToast();

  // Persist current page to localStorage
  useEffect(() => {
    try {
      localStorage.setItem("currentPage", page);
    } catch {
      // Silently fail if localStorage is unavailable
    }
  }, [page]);

  useEffect(() => {
    const onResize = () => {
      const mobile = window.innerWidth <= 1024;
      setIsMobile(mobile);
      if (!mobile) setIsSidebarOpen(false);
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  useEffect(() => {
    const handler = () => setPageKey((k) => k + 1);
    window.addEventListener("store-switched", handler);
    return () => window.removeEventListener("store-switched", handler);
  }, []);

  const pageTitle = useMemo(() => {
    const labels = {
      products: "Products",
      upload: "Upload",
      collections: "Collections",
      inventory: "Inventory",
      export: "Export",
      connect: "Connect Store",
      users: "User Management",
      metafields: "Metafields",
    };
    return labels[page] || "Dashboard";
  }, [page]);

  const handleSetPage = (nextPage) => {
    setPage(nextPage);
    if (isMobile) setIsSidebarOpen(false);
  };

  const handleStoreConnected = (data) => {
    setActiveStore({
      shop_key: data.shop_key,
      shop_name: data.shop_name,
      shop: data.shop,
      is_active: true,
    });
    add(`Connected to ${data.shop_name || data.shop}!`, "success");
    setPage("products");
    setPageKey((k) => k + 1);
  };

  const handleSignOut = async () => {
    await signOut();
    add("Signed out successfully.", "success");
  };

  // Loading screen
  if (loading) {
    return (
      <>
        <GlobalStyles />
        <div
          style={{
            minHeight: "100vh",
            display: "grid",
            placeItems: "center",
            color: "var(--text-secondary)",
            fontWeight: 600,
          }}
        >
          Checking authentication...
        </div>
      </>
    );
  }

  // Not authenticated - show AuthPage
  if (!user) {
    return (
      <>
        <GlobalStyles />
        <AuthPage onAuth={() => {}} />
      </>
    );
  }

  // User logged in but no role selected yet - show role selector
  if (!role) {
    return (
      <>
        <GlobalStyles />
        <RoleSelector user={user} />
      </>
    );
  }

  // Render page with permission checks
  const renderPage = () => {
    switch (page) {
      case "products":
        return can("manage_products") ? (
          <Suspense fallback={<PageLoader />}>
            <ProductsPage key={`products-${pageKey}`} toast={add} activeStore={activeStore} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "upload":
        return can("manage_upload") ? (
          <Suspense fallback={<PageLoader />}>
            <UploadPage key={`upload-${pageKey}`} toast={add} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "collections":
        return can("manage_collections") ? (
          <Suspense fallback={<PageLoader />}>
            <CollectionsPage key={`collections-${pageKey}`} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "inventory":
        return can("manage_inventory") ? (
          <Suspense fallback={<PageLoader />}>
            <InventoryPage key={`inventory-${pageKey}`} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "export":
        return can("manage_export") ? (
          <Suspense fallback={<PageLoader />}>
            <ExportPage key={`export-${pageKey}`} toast={add} activeStore={activeStore} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "metafields":
        return can("manage_metafields") ? (
          <Suspense fallback={<PageLoader />}>
            <MetafieldsPage key={`metafields-${pageKey}`} toast={add} activeStore={activeStore} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "connect":
        return can("manage_stores") ? (
          <Suspense fallback={<PageLoader />}>
            <ConnectStore onConnected={handleStoreConnected} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      case "users":
        return can("manage_users") ? (
          <Suspense fallback={<PageLoader />}>
            <UserManagementPage key={`users-${pageKey}`} toast={add} activeStore={activeStore} userEmail={user?.email} />
          </Suspense>
        ) : (
          <AccessDenied />
        );
      default:
        return (
          <Suspense fallback={<PageLoader />}>
            <ProductsPage key={`products-${pageKey}`} toast={add} activeStore={activeStore} />
          </Suspense>
        );
    }
  };

  return (
    <>
      <GlobalStyles />
      <Toasts toasts={toasts} remove={remove} />
      <div className="app-shell">
        <Sidebar
          page={page}
          setPage={handleSetPage}
          isMobile={isMobile}
          isOpen={isSidebarOpen}
          onClose={() => setIsSidebarOpen(false)}
          activeStore={activeStore}
          setActiveStore={setActiveStore}
          userEmail={user?.email}
          onSignOut={handleSignOut}
          user={user}
          can={can}
        />
        {isMobile && isSidebarOpen && (
          <button
            type="button"
            className="app-sidebar-backdrop"
            onClick={() => setIsSidebarOpen(false)}
            aria-label="Close navigation"
          />
        )}
        <main className="app-main">
          {isMobile && (
            <header className="app-mobile-header">
              <button
                type="button"
                className="app-mobile-menu-btn"
                onClick={() => setIsSidebarOpen((prev) => !prev)}
                aria-label="Toggle navigation"
              >
                <Ico n="menu" size={18} />
              </button>
              <h1>{pageTitle}</h1>
            </header>
          )}

          {renderPage()}
        </main>
      </div>
    </>
  );
}

// ── Role Selector Component ────────────────────────────────────────────────────
const ROLES = {
  admin:   { label: "Administrator", color: "#ef4444", bg: "rgba(239,68,68,0.12)",   desc: "Full access to all features and team management" },
  manager: { label: "Manager", color: "#f59e0b", bg: "rgba(245,158,11,0.12)",  desc: "All features except user management" },
  junior:  { label: "Junior", color: "#6366f1", bg: "rgba(99,102,241,0.12)",  desc: "Access only to assigned features" },
};

function RoleSelector({ user }) {
  const [selectedRole, setSelectedRole] = useState(null);
  const [adminEmail, setAdminEmail] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const { signOut } = useAuth();

  const handleSelectRole = async (roleKey) => {
    try {
      setLoading(true);
      setError("");
      setSelectedRole(roleKey);

      const { data: session } = await supabase.auth.getSession();
      const token = session?.session?.access_token;

      if (!token) {
        throw new Error("No auth token found");
      }

      if ((roleKey === "manager" || roleKey === "junior") && !adminEmail.trim()) {
        throw new Error("Admin email is required for manager or junior role");
      }

      const apiOrigin = (import.meta.env.VITE_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/$/, "");

      // Update user role on backend using the /me/role endpoint (allows users to set own role on first registration)
      const res = await fetch(`${apiOrigin}/api/users/me/role`, {
        method: "PUT",
        headers: {
          "Content-Type": "application/json",
          Authorization: `Bearer ${token}`,
        },
        body: JSON.stringify({
          role: roleKey,
          admin_email: (roleKey === "manager" || roleKey === "junior") ? adminEmail.trim().toLowerCase() : null,
        }),
      });

      if (!res.ok) {
        const err = await res.json();
        throw new Error(err.detail || err.message || "Failed to set role");
      }

      // Reload page to refresh auth state
      window.location.reload();
    } catch (err) {
      console.error("Role selection error:", err);
      setError(err.message || "Failed to set role");
      setSelectedRole(null);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div
      style={{
        minHeight: "100vh",
        background: "var(--bg-primary)",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        padding: "20px",
      }}
    >
      <div
        style={{
          maxWidth: "600px",
          width: "100%",
          background: "var(--bg-card)",
          border: "1px solid var(--border-subtle)",
          borderRadius: "16px",
          padding: "40px",
        }}
      >
        <div style={{ textAlign: "center", marginBottom: "32px" }}>
          <div
            style={{
              fontSize: "48px",
              marginBottom: "16px",
            }}
          >
            👋
          </div>
          <h1
            style={{
              fontSize: "24px",
              fontWeight: 700,
              color: "var(--text-primary)",
              margin: "0 0 8px 0",
            }}
          >
            Welcome, {user?.email}!
          </h1>
          <p
            style={{
              fontSize: "14px",
              color: "var(--text-muted)",
              margin: "0 0 24px 0",
            }}
          >
            Select your role to get started. You can change this later in User Management.
          </p>
        </div>

        {error && (
          <div
            style={{
              padding: "12px 14px",
              background: "rgba(239, 68, 68, 0.1)",
              border: "1px solid rgba(239, 68, 68, 0.3)",
              borderRadius: "8px",
              color: "#ef4444",
              fontSize: "13px",
              marginBottom: "20px",
            }}
          >
            ❌ {error}
          </div>
        )}

        <div style={{ display: "flex", flexDirection: "column", gap: "12px", marginBottom: "28px" }}>
          {Object.entries(ROLES).map(([roleKey, roleInfo]) => (
            <button
              key={roleKey}
              onClick={() => handleSelectRole(roleKey)}
              disabled={loading}
              style={{
                padding: "20px",
                border: selectedRole === roleKey ? `2px solid ${roleInfo.color}` : "1px solid var(--border-subtle)",
                background: selectedRole === roleKey ? roleInfo.bg : "transparent",
                borderRadius: "12px",
                cursor: loading ? "not-allowed" : "pointer",
                textAlign: "left",
                transition: "all 0.2s",
                opacity: loading ? 0.6 : 1,
              }}
              onMouseEnter={(e) => {
                if (!loading && selectedRole !== roleKey) {
                  e.currentTarget.style.borderColor = roleInfo.color;
                  e.currentTarget.style.background = `${roleInfo.bg}40`;
                }
              }}
              onMouseLeave={(e) => {
                if (!loading && selectedRole !== roleKey) {
                  e.currentTarget.style.borderColor = "var(--border-subtle)";
                  e.currentTarget.style.background = "transparent";
                }
              }}
            >
              <div
                style={{
                  fontSize: "14px",
                  fontWeight: 700,
                  color: roleInfo.color,
                  marginBottom: "6px",
                }}
              >
                {selectedRole === roleKey && loading ? "Setting up..." : roleInfo.label}
              </div>
              <div
                style={{
                  fontSize: "13px",
                  color: "var(--text-muted)",
                }}
              >
                {roleInfo.desc}
              </div>
            </button>
          ))}
        </div>

        {(selectedRole === "manager" || selectedRole === "junior") && (
          <div style={{ marginBottom: "20px" }}>
            <label
              style={{
                display: "block",
                fontSize: "12px",
                fontWeight: 700,
                color: "var(--text-muted)",
                marginBottom: "8px",
                textTransform: "uppercase",
              }}
            >
              Admin Email
            </label>
            <input
              type="email"
              value={adminEmail}
              onChange={(e) => setAdminEmail(e.target.value)}
              placeholder="admin@company.com"
              style={{
                width: "100%",
                padding: "10px 12px",
                borderRadius: "8px",
                border: "1px solid var(--border-subtle)",
                background: "var(--bg-primary)",
                color: "var(--text-primary)",
                fontSize: "14px",
              }}
            />
          </div>
        )}

        <button
          onClick={async () => {
            await signOut();
          }}
          style={{
            width: "100%",
            padding: "11px",
            background: "transparent",
            border: "1px solid var(--border-subtle)",
            borderRadius: "8px",
            color: "var(--text-muted)",
            fontSize: "13px",
            fontWeight: 600,
            cursor: "pointer",
            transition: "all 0.2s",
          }}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = "#ef4444";
            e.currentTarget.style.color = "#ef4444";
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = "var(--border-subtle)";
            e.currentTarget.style.color = "var(--text-muted)";
          }}
        >
          Sign out
        </button>
      </div>
    </div>
  );
}
