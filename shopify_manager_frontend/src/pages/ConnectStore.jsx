import { useMemo, useState } from "react";
import { authFetch } from "../lib/authFetch";
import { api } from "../api/api";
import { useAuth } from "../hooks/useAuth";

// Fix: Ensure proper component structure

const rawApiOrigin = (import.meta.env.VITE_API_ORIGIN || "")
  .trim()
  .replace(/\/$/, "");

const normalizedApiOrigin = rawApiOrigin
  ? /^https?:\/\//i.test(rawApiOrigin)
    ? rawApiOrigin
    : `https://${rawApiOrigin}`
  : "";

const API = normalizedApiOrigin
  ? `${normalizedApiOrigin}/api`
  : "http://127.0.0.1:8000/api";

// Spinner component (unchanged but kept for completeness)
const Spin = ({ size = 18 }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="none"
    style={{ animation: "spin 0.8s linear infinite", flexShrink: 0 }}
  >
    <circle cx="12" cy="12" r="10" stroke="#3d3d5c" strokeWidth="3" />
    <path
      d="M12 2a10 10 0 0110 10"
      stroke="#6366f1"
      strokeWidth="3"
      strokeLinecap="round"
    />
  </svg>
);

// Checkmark icon for success screen
const Checkmark = ({ size = 24 }) => (
  <svg
    width={size}
    height={size}
    viewBox="0 0 24 24"
    fill="none"
    stroke="currentColor"
    strokeWidth="3"
    strokeLinecap="round"
    strokeLinejoin="round"
  >
    <polyline points="20 6 9 17 4 12" />
  </svg>
);

const versions = ["2026-01", "2025-10", "2025-07", "2025-04"];

// Panel style with fallbacks for CSS variables
const panelStyle = {
  background: "var(--panel-gradient)",
  border: "1px solid var(--border-strong, #2a2a35)",
  borderRadius: 24,
  boxShadow: "0 24px 80px rgba(0,0,0,0.35)",
};

function normalizeShopName(value) {
  return value
    .trim()
    .replace(/^https?:\/\//i, "")
    .replace(/\/.*$/, "")
    .replace(/\.myshopify\.com$/i, "")
    .replace(/\s+/g, "")
    .replace(/[^a-zA-Z0-9-]/g, "")
    .toLowerCase();
}

// Improved Field component with proper htmlFor and optional hint
function Field({ label, hint, children, id }) {
  return (
    <div style={{ display: "grid", gap: 6, minWidth: 0 }}>
      <label
        htmlFor={id}
        style={{
          fontSize: 11,
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          color: "var(--text-muted, #9ca3af)",
        }}
      >
        {label}
      </label>
      {children}
      {hint && (
        <span
          style={{
            fontSize: 12,
            color: "var(--text-muted, #9ca3af)",
            minWidth: 0,
            overflowWrap: "anywhere",
          }}
        >
          {hint}
        </span>
      )}
    </div>
  );
}

function Stat({ label, value }) {
  return (
    <div
      style={{
        border: "1px solid var(--panel-overlay-light)",
        borderRadius: 16,
        padding: "14px 16px",
        background: "var(--panel-overlay-very-subtle)",
        minWidth: 0,
      }}
    >
      <div
        style={{
          fontSize: 11,
          color: "var(--text-muted, #9ca3af)",
          textTransform: "uppercase",
          letterSpacing: "0.08em",
        }}
      >
        {label}
      </div>
      <div
        style={{
          marginTop: 4.5,
          fontSize: 13.5,
          fontWeight: 700,
          color: "var(--text-primary, #f3f4f6)",
          minWidth: 0,
          overflowWrap: "anywhere",
          wordBreak: "break-word",
        }}
      >
        {value}
      </div>
    </div>
  );
}

export default function ConnectStore({ onConnected }) {
  const { role, can } = useAuth();
  const [form, setForm] = useState({
    shop_name: "",
    api_key: "",
    api_secret: "",
    api_version: "2026-01",
  });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [success, setSuccess] = useState(null);
  const [showSecret, setShowSecret] = useState(false);

  const f = (k) => (v) => setForm((p) => ({ ...p, [k]: v }));

  const normalizedShopName = useMemo(
    () => normalizeShopName(form.shop_name),
    [form.shop_name],
  );

  const storeDomain = normalizedShopName
    ? `${normalizedShopName}.myshopify.com`
    : "your-store.myshopify.com";

  const handleConnect = async (event) => {
    event?.preventDefault();
    if (
      !normalizedShopName ||
      !form.api_key.trim() ||
      !form.api_secret.trim()
    ) {
      setError("All fields are required");
      return;
    }
    setLoading(true);
    setError("");
    try {
      const payload = {
        ...form,
        shop_name: normalizedShopName,
        api_key: form.api_key.trim(),
        api_secret: form.api_secret.trim(),
      };
      const data = await api.post(`/auth/connect`, payload);
      setSuccess(data);
      if (onConnected) onConnected(data);
    } catch (e) {
      const msg = e.message || "Failed to fetch - is the backend running?";
      setError(msg);
    }
    setLoading(false);
  };

  // For accessibility: generate unique IDs for inputs
  const fieldIds = {
    shop: "shop-name",
    apiKey: "api-key",
    apiSecret: "api-secret",
    apiVersion: "api-version",
  };

  // Check permissions
  const hasAccess = can("manage_stores");
  const hasRole = role !== null;

  if (!hasRole) {
    return (
      <div
        style={{
          minHeight: "calc(100vh - 60px)",
          display: "grid",
          placeItems: "center",
          padding: "18px",
        }}
      >
        <div
          style={{
            maxWidth: 540,
            width: "100%",
            textAlign: "center",
            background: "var(--bg-card)",
            border: "1px solid var(--border-subtle)",
            borderRadius: 12,
            padding: "40px 20px",
          }}
        >
          <div
            style={{
              fontSize: "48px",
              marginBottom: "16px",
            }}
          >
            🔒
          </div>
          <h2
            style={{
              fontSize: "24px",
              fontWeight: 700,
              color: "var(--text-primary)",
              margin: "0 0 8px 0",
            }}
          >
            Access Denied
          </h2>
          <p
            style={{
              color: "var(--text-secondary)",
              fontSize: "14px",
              margin: "8px 0 0 0",
              lineHeight: "1.5",
            }}
          >
            You must complete role-based login in the Team Management section to connect stores.
          </p>
        </div>
      </div>
    );
  }

  if (!hasAccess) {
    return (
      <div
        style={{
          minHeight: "calc(100vh - 60px)",
          display: "grid",
          placeItems: "center",
          padding: "18px",
        }}
      >
        <div
          style={{
            maxWidth: 540,
            width: "100%",
            textAlign: "center",
            background: "var(--bg-card)",
            border: "1px solid var(--border-subtle)",
            borderRadius: 12,
            padding: "40px 20px",
          }}
        >
          <div
            style={{
              fontSize: "48px",
              marginBottom: "16px",
            }}
          >
            ⛔
          </div>
          <h2
            style={{
              fontSize: "24px",
              fontWeight: 700,
              color: "var(--text-primary)",
              margin: "0 0 8px 0",
            }}
          >
            Permission Denied
          </h2>
          <p
            style={{
              color: "var(--text-secondary)",
              fontSize: "14px",
              margin: "8px 0 0 0",
              lineHeight: "1.5",
            }}
          >
            Your role does not have permission to manage stores. Contact your administrator for access.
          </p>
        </div>
      </div>
    );
  }

  if (success) {
    return (
      <div
        style={{
          minHeight: "calc(100vh - 60px)",
          display: "grid",
          placeItems: "center",
          padding: "18px",
        }}
      >
        <div
          style={{
            ...panelStyle,
            maxWidth: 540,
            width: "100%",
            padding: "clamp(18px, 3vw, 30px)",
            position: "relative",
            overflow: "hidden",
          }}
        >
          <div
            style={{
              position: "absolute",
              inset: "auto -80px -100px auto",
              width: 220,
              height: 220,
              borderRadius: "50%",
              background: "var(--success-gradient)",
            }}
          />
          <div
            style={{
              width: 48,
              height: 48,
              borderRadius: 20,
              display: "grid",
              placeItems: "center",
              background: "var(--success-icon-gradient)",
              border: "1px solid var(--success-icon-border)",
              color: "var(--success-text)",
              fontSize: 22.5,
              marginBottom: 13.5,
            }}
          >
            <Checkmark size={30} />
          </div>
          <div style={{ maxWidth: 540, position: "relative" }}>
            <div
              style={{
                fontSize: 12,
                fontWeight: 700,
                letterSpacing: "0.1em",
                textTransform: "uppercase",
                color: "var(--success-text)",
              }}
            >
              Store Ready
            </div>
            <h1
              style={{
                marginTop: 10,
                fontFamily: "var(--font-display, 'Inter', sans-serif)",
                fontSize: "clamp(1.8rem, 3vw, 2.5rem)",
                lineHeight: 1.05,
              }}
            >
              {success.shop_name || success.shop || "Store"} is connected.
            </h1>
            <p
              style={{
                marginTop: 12,
                color: "var(--text-secondary, #9ca3af)",
                maxWidth: 500,
              }}
            >
              Your credentials were verified and the store is now available
              inside the dashboard.
            </p>
          </div>

          <div
            style={{
              display: "grid",
              gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
              gap: 12,
              marginTop: 24,
            }}
          >
            <Stat
              label="Store"
              value={success.shop_name || success.shop || "-"}
            />
            <Stat
              label="API Version"
              value={success.api_version || form.api_version}
            />
            <Stat label="Token" value={success.token_preview || "Available"} />
          </div>

          {success.auto_sync && (
            <div
              style={{
                marginTop: 18,
                padding: "14px 16px",
                borderRadius: 16,
                background: success.auto_sync.success
                  ? "var(--success-light)"
                  : "var(--error-bg-light)",
                border: success.auto_sync.success
                  ? "1px solid var(--success-border-light)"
                  : "1px solid var(--error-border)",
                color: success.auto_sync.success
                  ? "var(--success-text-light)"
                  : "var(--error-text-light)",
              }}
            >
              {success.auto_sync.success
                ? `Auto-synced ${success.auto_sync.synced_count} products.`
                : `Auto-sync failed: ${success.auto_sync.error}`}
            </div>
          )}

          <div
            style={{
              display: "flex",
              gap: 12,
              flexWrap: "wrap",
              marginTop: 24,
            }}
          >
            <button
              onClick={() => {
                // Use router if available, otherwise reload
                if (window.__router?.navigate) {
                  window.__router.navigate("/dashboard");
                } else {
                  window.location.href = "/dashboard";
                }
              }}
              style={{
                minWidth: 220,
                padding: "14px 18px",
                borderRadius: 14,
                fontSize: 15,
                fontWeight: 700,
                background:
                  "var(--accent-gradient, linear-gradient(135deg, #6366f1, #8b5cf6))",
                border: "none",
                color: "#fff",
                cursor: "pointer",
                transition: "opacity 0.2s ease",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.opacity = 0.9)}
              onMouseLeave={(e) => (e.currentTarget.style.opacity = 1)}
              onFocus={(e) =>
                (e.currentTarget.style.outline = "2px solid #fff")
              }
            >
              Open dashboard
            </button>
            <button
              onClick={() => setSuccess(null)}
              style={{
                padding: "14px 18px",
                borderRadius: 14,
                fontSize: 15,
                fontWeight: 700,
                background: "transparent",
                border: "1px solid var(--border-strong, #2a2a35)",
                color: "var(--text-secondary, #9ca3af)",
                cursor: "pointer",
                transition:
                  "background-color 0.2s ease, border-color 0.2s ease",
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.backgroundColor =
                  "var(--panel-overlay-subtle)";
                e.currentTarget.style.borderColor = "var(--accent, #6366f1)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.backgroundColor = "transparent";
                e.currentTarget.style.borderColor =
                  "var(--border-strong, #2a2a35)";
              }}
              onFocus={(e) =>
                (e.currentTarget.style.outline = "2px solid #fff")
              }
            >
              Connect another store
            </button>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      style={{
        minHeight: "calc(100vh - 80px)",
        width: "100%",
        padding: "clamp(16px, 3vw, 28px)",
      }}
    >
      <style>{`
        @keyframes spin { to { transform: rotate(360deg); } }
        .connect-shell {
          display: grid;
          grid-template-columns: minmax(280px, 1.05fr) minmax(320px, 0.95fr);
          gap: 18px;
          align-items: stretch;
          min-height: calc(100vh - 136px);
        }
        .connect-shell > * {
          min-width: 0;
        }
        .connect-input,
        .connect-select {
          width: 100%;
          min-width: 0;
          padding: 13px 14px;
          box-sizing: border-box;
          border-radius: 14px;
          border: 1px solid var(--border-strong, #2a2a35);
          background: var(--panel-overlay-very-subtle);
          color: var(--text-primary, #f3f4f6);
          outline: none;
          transition: border-color .2s ease, box-shadow .2s ease, background .2s ease;
          font-size: 14px;
        }
        .connect-input::placeholder {
          color: var(--text-muted, #9ca3af);
        }
        .connect-input:focus,
        .connect-select:focus {
          border-color: var(--accent, #6366f1);
          box-shadow: var(--shadow-focus);
          background: var(--panel-overlay-subtle);
        }
        .connect-select {
          appearance: none;
          cursor: pointer;
          background-image: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='16' height='16' viewBox='0 0 24 24' fill='none' stroke='%239ca3af' stroke-width='2' stroke-linecap='round' stroke-linejoin='round'%3E%3Cpolyline points='6 9 12 15 18 9'%3E%3C/polyline%3E%3C/svg%3E");
          background-repeat: no-repeat;
          background-position: right 14px center;
          background-size: 16px;
          padding-right: 40px;
        }
        .connect-store-input {
          display: grid;
          grid-template-columns: minmax(0, 1fr) auto;
          align-items: center;
          min-width: 0;
          border-radius: 14px;
          border: 1px solid var(--border-strong, #2a2a35);
          background: var(--panel-overlay-very-subtle);
          overflow: hidden;
          transition: border-color .2s ease, box-shadow .2s ease, background .2s ease;
        }
        .connect-store-input:focus-within {
          border-color: var(--accent, #6366f1);
          box-shadow: var(--shadow-focus);
          background: var(--panel-overlay-subtle);
        }
        .connect-store-input .connect-input {
          border: 0;
          background: transparent;
          box-shadow: none;
          min-width: 0;
          width: 100%;
        }
        .connect-store-input .connect-input:focus {
          border-color: transparent;
          box-shadow: none;
          background: transparent;
        }
        .connect-store-suffix {
          padding: 0 14px;
          color: var(--text-muted, #9ca3af);
          font-size: 13px;
          white-space: nowrap;
          border-left: 1px solid var(--panel-overlay-light);
          background: var(--panel-overlay-very-subtle);
          height: 100%;
          display: inline-flex;
          align-items: center;
        }
        @media (max-width: 980px) {
          .connect-shell {
            grid-template-columns: 1fr;
            min-height: auto;
          }
          .connect-store-input {
            grid-template-columns: 1fr;
          }
          .connect-store-suffix {
            border-left: 0;
            border-top: 1px solid var(--panel-overlay-light);
            justify-content: flex-start;
            padding: 10px 14px;
          }
        }
      `}</style>

      <div className="connect-shell">
        <section
          style={{
            ...panelStyle,
            padding: "clamp(24px, 4vw, 38px)",
            position: "relative",
            overflow: "hidden",
            display: "flex",
            flexDirection: "column",
            justifyContent: "space-between",
            minHeight: 520,
          }}
        >
          <div
            style={{
              position: "absolute",
              top: -80,
              right: -70,
              width: 240,
              height: 240,
              borderRadius: "50%",
              background: "var(--accent-gradient-bg)",
            }}
          />
          <div
            style={{
              position: "absolute",
              bottom: -90,
              left: -60,
              width: 220,
              height: 220,
              borderRadius: "50%",
              background: "var(--purple-gradient-bg)",
            }}
          />

          <div style={{ position: "relative" }}>
            <div
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 8,
                padding: "8px 12px",
                borderRadius: 999,
                background: "var(--panel-overlay-subtle)",
                border: "1px solid var(--panel-overlay-light)",
                color: "var(--text-secondary, #9ca3af)",
                fontSize: 12,
                fontWeight: 700,
                letterSpacing: "0.06em",
                textTransform: "uppercase",
              }}
            >
              Shopify Admin
            </div>

            <h1
              style={{
                marginTop: 20,
                fontFamily: "var(--font-display, 'Inter', sans-serif)",
                fontSize: "clamp(2.2rem, 4vw, 3.5rem)",
                lineHeight: 0.95,
                maxWidth: 560,
              }}
            >
              Bring a store online in one clean step.
            </h1>
            <p
              style={{
                marginTop: 16,
                maxWidth: 520,
                color: "var(--text-secondary, #9ca3af)",
                fontSize: "clamp(0.95rem, 1.6vw, 1.05rem)",
              }}
            >
              Add your store credentials, confirm the API version, and the
              dashboard will wire the shop into products, inventory,
              collections, and export flows.
            </p>
          </div>

          <div
            style={{
              position: "relative",
              display: "grid",
              gap: 12,
              marginTop: 24,
            }}
          >
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(auto-fit, minmax(140px, 1fr))",
                gap: 12,
              }}
            >
              <Stat label="Preview domain" value={storeDomain} />
              <Stat label="Default API" value={form.api_version} />
            </div>

            <div
              style={{
                borderRadius: 18,
                padding: "18px 18px 16px",
                border: "1px solid var(--panel-overlay-light)",
                background: "var(--panel-overlay-very-subtle)",
              }}
            >
              <div
                style={{
                  fontSize: 12,
                  fontWeight: 700,
                  letterSpacing: "0.08em",
                  textTransform: "uppercase",
                  color: "var(--text-muted, #9ca3af)",
                }}
              >
                What you need
              </div>
              <div style={{ display: "grid", gap: 10, marginTop: 14 }}>
                {[
                  "Store name without the .myshopify.com suffix",
                  "Admin API key from a custom app",
                  "Admin access token or app secret used by your backend",
                ].map((item) => (
                  <div
                    key={item}
                    style={{
                      display: "flex",
                      gap: 10,
                      color: "var(--text-secondary, #9ca3af)",
                    }}
                  >
                    <span style={{ color: "var(--accent-text)" }}>+</span>
                    <span>{item}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </section>

        <section
          style={{
            ...panelStyle,
            padding: "clamp(22px, 3vw, 34px)",
            display: "grid",
            alignContent: "start",
            gap: 22,
          }}
        >
          <div>
            <div
              style={{
                fontSize: 12,
                fontWeight: 700,
                letterSpacing: "0.08em",
                textTransform: "uppercase",
                color: "var(--accent-text)",
              }}
            >
              Connect Store
            </div>
            <h2
              style={{
                marginTop: 10,
                fontSize: "clamp(1.5rem, 2vw, 2rem)",
                lineHeight: 1.05,
              }}
            >
              Credentials
            </h2>
            <p
              style={{ marginTop: 8, color: "var(--text-secondary, #9ca3af)" }}
            >
              The backend validates these values against Shopify before
              activating the store.
            </p>
          </div>

          <div
            style={{
              borderRadius: 18,
              border: "1px solid var(--info-box-border)",
              background: "var(--info-box-bg)",
              padding: "16px 18px",
            }}
          >
            <div
              style={{
                fontSize: 12,
                fontWeight: 700,
                color: "var(--info-box-label)",
                marginBottom: 6,
              }}
            >
              Where to find them
            </div>
            <div
              style={{
                fontSize: 13,
                color: "var(--info-box-text)",
                lineHeight: 1.7,
              }}
            >
              {
                "Shopify Admin -> Settings -> Apps and sales channels -> Develop apps -> Create app -> Admin API credentials"
              }
            </div>
          </div>

          <form onSubmit={handleConnect} style={{ display: "grid", gap: 16 }}>
            <Field
              label="Store name"
              hint={
                normalizedShopName
                  ? `Preview: ${storeDomain}`
                  : "Enter only the store slug. Full URLs are cleaned automatically."
              }
              id={fieldIds.shop}
            >
              <div className="connect-store-input">
                <input
                  id={fieldIds.shop}
                  className="connect-input"
                  placeholder="mystore"
                  value={form.shop_name}
                  autoCapitalize="none"
                  autoCorrect="off"
                  spellCheck={false}
                  onChange={(e) => {
                    setError("");
                    f("shop_name")(e.target.value);
                  }}
                  onBlur={(e) =>
                    f("shop_name")(normalizeShopName(e.target.value))
                  }
                />
                <span className="connect-store-suffix">.myshopify.com</span>
              </div>
            </Field>

            <Field
              label="API key"
              hint="Public identifier for the custom app"
              id={fieldIds.apiKey}
            >
              <input
                id={fieldIds.apiKey}
                className="connect-input"
                placeholder="e36895244cf2e5be85d9..."
                value={form.api_key}
                autoCapitalize="none"
                autoCorrect="off"
                spellCheck={false}
                onChange={(e) => {
                  setError("");
                  f("api_key")(e.target.value);
                }}
              />
            </Field>

            <Field
              label="API secret or access token"
              hint="Stored by the backend and used for Admin API access"
              id={fieldIds.apiSecret}
            >
              <div style={{ position: "relative" }}>
                <input
                  id={fieldIds.apiSecret}
                  className="connect-input"
                  type={showSecret ? "text" : "password"}
                  placeholder="shpss_... or shpua_..."
                  value={form.api_secret}
                  autoComplete="new-password"
                  autoCapitalize="none"
                  autoCorrect="off"
                  spellCheck={false}
                  onChange={(e) => {
                    setError("");
                    f("api_secret")(e.target.value);
                  }}
                  style={{ paddingRight: 90 }}
                />
                <button
                  type="button"
                  onClick={() => setShowSecret((v) => !v)}
                  aria-pressed={showSecret}
                  style={{
                    position: "absolute",
                    top: "50%",
                    transform: "translateY(-50%)",
                    right: 8,
                    height: 36,
                    padding: "0 12px",
                    borderRadius: 10,
                    border: "1px solid var(--border-strong, #2a2a35)",
                    background: "var(--panel-overlay-minimal)",
                    color: "var(--text-secondary, #9ca3af)",
                    cursor: "pointer",
                    fontWeight: 700,
                    transition: "background-color 0.2s ease",
                  }}
                  onMouseEnter={(e) =>
                    (e.currentTarget.style.backgroundColor =
                      "var(--panel-overlay-subtle)")
                  }
                  onMouseLeave={(e) =>
                    (e.currentTarget.style.backgroundColor =
                      "var(--panel-overlay-minimal)")
                  }
                  onFocus={(e) =>
                    (e.currentTarget.style.outline = "2px solid #fff")
                  }
                >
                  {showSecret ? "Hide" : "Show"}
                </button>
              </div>
            </Field>

            <Field
              label="API version"
              hint="Use the latest stable version unless this store is pinned to an older release"
              id={fieldIds.apiVersion}
            >
              <select
                id={fieldIds.apiVersion}
                className="connect-select"
                value={form.api_version}
                onChange={(e) => f("api_version")(e.target.value)}
              >
                {versions.map((version) => (
                  <option key={version} value={version}>
                    {version}
                    {version === "2026-01" ? " (Latest)" : ""}
                  </option>
                ))}
              </select>
            </Field>

            {error && (
              <div
                style={{
                  background: "var(--error-bg-light)",
                  border: "1px solid var(--error-border-light)",
                  borderRadius: 14,
                  padding: "12px 14px",
                  fontSize: 13,
                  color: "var(--error-text-light)",
                  display: "flex",
                  gap: 8,
                  alignItems: "center",
                }}
              >
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="currentColor"
                  strokeWidth="2"
                >
                  <circle cx="12" cy="12" r="10" />
                  <line x1="12" y1="8" x2="12" y2="12" />
                  <line x1="12" y1="16" x2="12.01" y2="16" />
                </svg>
                {error}
              </div>
            )}

            <button
              type="submit"
              disabled={loading}
              style={{
                width: "100%",
                padding: "15px 16px",
                borderRadius: 16,
                fontSize: 15,
                fontWeight: 800,
                background: loading
                  ? "var(--panel-overlay-light)"
                  : "var(--accent-gradient, linear-gradient(135deg, #6366f1, #8b5cf6))",
                border: "none",
                color: loading ? "var(--text-muted, #9ca3af)" : "#fff",
                cursor: loading ? "not-allowed" : "pointer",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                gap: 8,
                boxShadow: loading ? "none" : "var(--accent-button-shadow)",
                transition: "opacity 0.2s ease",
              }}
              onMouseEnter={(e) => {
                if (!loading) e.currentTarget.style.opacity = 0.9;
              }}
              onMouseLeave={(e) => {
                if (!loading) e.currentTarget.style.opacity = 1;
              }}
              onFocus={(e) =>
                (e.currentTarget.style.outline = "2px solid #fff")
              }
            >
              {loading ? (
                <>
                  <Spin size={16} /> Connecting...
                </>
              ) : (
                "Connect store"
              )}
            </button>
          </form>
        </section>
      </div>
    </div>
  );
}
