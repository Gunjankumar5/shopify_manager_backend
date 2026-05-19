import { useState, useEffect } from "react";
import { supabase } from "../lib/supabaseClient";

// ── Views ─────────────────────────────────────────────────────────────────────
const VIEW = {
  LOGIN:   "login",
  SIGNUP:  "signup",
  FORGOT:  "forgot",
  RESET:   "reset",    // password reset (from email link)
  PENDING: "pending",  // email confirmation pending
};

// ── Styles ────────────────────────────────────────────────────────────────────
const S = {
  page: {
    minHeight: "100vh",
    background:
      "radial-gradient(circle at top left, rgba(99,102,241,0.24), transparent 28%), radial-gradient(circle at bottom right, rgba(16,185,129,0.16), transparent 34%), linear-gradient(135deg, #08090f 0%, #111423 48%, #0d111c 100%)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    padding: "28px",
    fontFamily: "inherit",
    position: "relative",
    overflow: "hidden",
  },
  card: {
    background: "linear-gradient(180deg, rgba(18,22,36,0.96), rgba(11,13,22,0.98))",
    backdropFilter: "blur(18px)",
    border: "1px solid rgba(255,255,255,0.08)",
    borderRadius: 30,
    padding: "34px",
    width: "100%",
    maxWidth: 520,
    boxShadow: "0 28px 90px rgba(0,0,0,0.58), 0 0 0 1px rgba(255,255,255,0.03)",
  },
  logo: {
    width: 64,
    height: 64,
    borderRadius: 18,
    background: "linear-gradient(135deg, #6366f1 0%, #8b5cf6 55%, #22c55e 135%)",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: 28,
    margin: "0 auto 18px",
    boxShadow: "0 18px 42px rgba(99,102,241,0.34)",
    border: "1px solid rgba(255,255,255,0.12)",
  },
  title: {
    fontSize: 30,
    fontWeight: 800,
    color: "var(--text-primary)",
    textAlign: "center",
    marginBottom: 8,
    letterSpacing: "-0.04em",
    fontFamily: "var(--font-display)",
  },
  subtitle: {
    fontSize: 14,
    color: "var(--text-secondary)",
    textAlign: "center",
    marginBottom: 22,
    lineHeight: 1.5,
  },
  eyebrow: {
    display: "inline-flex",
    alignItems: "center",
    gap: 8,
    padding: "6px 12px",
    borderRadius: 999,
    background: "rgba(99,102,241,0.12)",
    border: "1px solid rgba(99,102,241,0.2)",
    color: "#c7d2fe",
    fontSize: 11,
    fontWeight: 700,
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    margin: "0 auto 14px",
  },
  metaRow: {
    display: "flex",
    flexWrap: "wrap",
    justifyContent: "center",
    gap: 8,
    marginBottom: 24,
  },
  metaPill: {
    padding: "7px 10px",
    borderRadius: 999,
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.08)",
    color: "var(--text-secondary)",
    fontSize: 11,
    fontWeight: 600,
  },
  label: {
    display: "block",
    fontSize: 11,
    fontWeight: 700,
    color: "var(--text-muted)",
    textTransform: "uppercase",
    letterSpacing: "0.1em",
    marginBottom: 8,
  },
  input: {
    width: "100%",
    padding: "13px 16px",
    background: "rgba(255,255,255,0.04)",
    border: "1px solid rgba(255,255,255,0.09)",
    borderRadius: 16,
    fontSize: 14,
    color: "var(--text-primary)",
    outline: "none",
    fontFamily: "inherit",
    boxSizing: "border-box",
    transition: "border-color 0.2s, box-shadow 0.2s, transform 0.2s, background 0.2s",
    boxShadow: "inset 0 1px 0 rgba(255,255,255,0.02)",
  },
  btn: {
    width: "100%",
    padding: "14px 16px",
    borderRadius: 16,
    fontSize: 14,
    fontWeight: 700,
    background: "var(--accent-gradient)",
    border: "none",
    color: "var(--text-on-accent)",
    cursor: "pointer",
    fontFamily: "inherit",
    transition: "transform 0.2s, box-shadow 0.2s, opacity 0.2s",
    marginTop: 10,
    boxShadow: "0 16px 32px rgba(99,102,241,0.28)",
  },
  btnDisabled: {
    opacity: 0.6,
    cursor: "not-allowed",
  },
  btnSecondary: {
    width: "100%",
    padding: "13px 16px",
    borderRadius: 16,
    fontSize: 14,
    fontWeight: 600,
    background: "rgba(255,255,255,0.03)",
    border: "1px solid rgba(255,255,255,0.08)",
    color: "var(--text-primary)",
    cursor: "pointer",
    fontFamily: "inherit",
    transition: "all 0.2s",
    marginTop: 10,
  },
  btnText: {
    width: "100%",
    padding: "13px",
    borderRadius: 14,
    fontSize: 14,
    fontWeight: 600,
    background: "transparent",
    border: "1px solid rgba(255,255,255,0.08)",
    color: "var(--text-muted)",
    cursor: "pointer",
    fontFamily: "inherit",
    marginTop: 10,
    transition: "all 0.2s",
  },
  divider: {
    display: "flex",
    alignItems: "center",
    gap: 12,
    margin: "20px 0 18px",
    color: "var(--text-muted)",
    fontSize: 12,
  },
  dividerLine: {
    flex: 1,
    height: 1,
    background: "rgba(255,255,255,0.08)",
  },
  error: {
    background: "rgba(239,68,68,0.11)",
    border: "1px solid rgba(239,68,68,0.22)",
    borderRadius: 16,
    padding: "12px 16px",
    fontSize: 13,
    color: "#fecaca",
    marginBottom: 18,
    fontWeight: 500,
    lineHeight: 1.5,
  },
  success: {
    background: "rgba(16,185,129,0.11)",
    border: "1px solid rgba(16,185,129,0.22)",
    borderRadius: 16,
    padding: "12px 16px",
    fontSize: 13,
    color: "#bbf7d0",
    marginBottom: 18,
    fontWeight: 500,
    lineHeight: 1.5,
  },
  link: {
    background: "none",
    border: "none",
    color: "#93c5fd",
    cursor: "pointer",
    fontSize: 13,
    padding: 0,
    fontFamily: "inherit",
    fontWeight: 600,
  },
  googleBtn: {
    width: "100%",
    padding: "13px 16px",
    borderRadius: 16,
    border: "1px solid rgba(255,255,255,0.08)",
    background: "rgba(255,255,255,0.04)",
    color: "var(--text-primary)",
    fontSize: 14,
    fontWeight: 600,
    cursor: "pointer",
    fontFamily: "inherit",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    gap: 10,
    marginBottom: 12,
    transition: "all 0.2s",
  },
};

// ── Google SVG ────────────────────────────────────────────────────────────────
const GoogleIcon = () => (
  <svg width="18" height="18" viewBox="0 0 48 48">
    <path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/>
    <path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/>
    <path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/>
    <path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/>
  </svg>
);

// ── Spinner ───────────────────────────────────────────────────────────────────
const Spin = () => (
  <svg width="16" height="16" viewBox="0 0 24 24" fill="none"
    style={{ animation: "spin 0.8s linear infinite" }}>
    <circle cx="12" cy="12" r="10" stroke="#3d3d5c" strokeWidth="3"/>
    <path d="M12 2a10 10 0 0110 10" stroke="#fff" strokeWidth="3" strokeLinecap="round"/>
    <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
  </svg>
);

// ── Input component ───────────────────────────────────────────────────────────
function Input({ label, type = "text", value, onChange, placeholder, autoComplete }) {
  const [focused, setFocused] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const isPasswordField = type === "password";
  const inputType = isPasswordField && showPassword ? "text" : type;

  return (
    <div style={{ marginBottom: 16 }}>
      {label && <label style={S.label}>{label}</label>}
      <div style={{ position: "relative" }}>
        <input
          type={inputType}
          value={value}
          onChange={onChange}
          placeholder={placeholder}
          autoComplete={autoComplete}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          style={{
            ...S.input,
            borderColor: focused ? "#3B82F6" : "#2a2a3d",
            boxShadow: focused ? "0 0 0 3px rgba(59,130,246,0.1)" : "none",
            paddingRight: isPasswordField ? "40px" : "14px",
          }}
        />
        {isPasswordField && (
          <button
            type="button"
            onClick={() => setShowPassword(!showPassword)}
            style={{
              position: "absolute",
              right: "12px",
              top: "50%",
              transform: "translateY(-50%)",
              background: "none",
              border: "none",
              cursor: "pointer",
              color: focused ? "#3B82F6" : "#666",
              fontSize: "18px",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: "4px",
              transition: "color 0.2s",
            }}
            title={showPassword ? "Hide password" : "Show password"}
          >
            {showPassword ? "👁️" : "👁️‍🗨️"}
          </button>
        )}
      </div>
    </div>
  );
}

// ── Main AuthPage ─────────────────────────────────────────────────────────────
export default function AuthPage({ onAuth }) {
  const [view,     setView]     = useState(VIEW.LOGIN);
  const [email,    setEmail]    = useState("");
  const [password, setPassword] = useState("");
  const [confirm,  setConfirm]  = useState("");
  const [fullName, setFullName] = useState("");
  const [newPass,  setNewPass]  = useState("");
  const [newConf,  setNewConf]  = useState("");
  const [signupRole, setSignupRole] = useState("admin");
  const [adminEmail, setAdminEmail] = useState("");
  const [loading,  setLoading]  = useState(false);
  const [error,    setError]    = useState("");
  const [success,  setSuccess]  = useState("");

  // ── Detect password reset link (from email) ───────────────────────────────
  useEffect(() => {
    const hash = window.location.hash;
    if (hash.includes("type=recovery")) {
      setView(VIEW.RESET);
    }
  }, []);

  const clear = () => { setError(""); setSuccess(""); };

  // ── Login ─────────────────────────────────────────────────────────────────
  const handleLogin = async () => {
    if (!email || !password) { setError("Email and password are required"); return; }
    setLoading(true); clear();
    const { error } = await supabase.auth.signInWithPassword({ email, password });
    if (error) setError(error.message);
    else onAuth?.();
    setLoading(false);
  };

  // ── Signup ────────────────────────────────────────────────────────────────
  const handleSignup = async () => {
    if (!fullName || !email || !password) { setError("All fields are required"); return; }
    if (password !== confirm) { setError("Passwords do not match"); return; }
    if (password.length < 6)  { setError("Password must be at least 6 characters"); return; }
    if ((signupRole === "manager" || signupRole === "junior") && !adminEmail.trim()) {
      setError("Admin email is required for manager or junior signup");
      return;
    }
    setLoading(true); clear();
    const { error } = await supabase.auth.signUp({
      email,
      password,
      options: {
        data: {
          full_name: fullName,
          desired_role: signupRole,
          admin_email: (signupRole === "manager" || signupRole === "junior")
            ? adminEmail.trim().toLowerCase()
            : null,
        },
      },
    });
    if (error) setError(error.message);
    else {
      setView(VIEW.PENDING);
      setSuccess("Check your email to confirm your account!");
    }
    setLoading(false);
  };

  // ── Forgot password ───────────────────────────────────────────────────────
  const handleForgot = async () => {
    if (!email) { setError("Enter your email address"); return; }
    setLoading(true); clear();
    const { error } = await supabase.auth.resetPasswordForEmail(email, {
      redirectTo: `${window.location.origin}`,
    });
    if (error) setError(error.message);
    else setSuccess("Password reset email sent! Check your inbox.");
    setLoading(false);
  };

  // ── Reset password ────────────────────────────────────────────────────────
  const handleReset = async () => {
    if (!newPass) { setError("Enter a new password"); return; }
    if (newPass !== newConf) { setError("Passwords do not match"); return; }
    if (newPass.length < 6) { setError("Password must be at least 6 characters"); return; }
    setLoading(true); clear();
    const { error } = await supabase.auth.updateUser({ password: newPass });
    if (error) setError(error.message);
    else {
      setSuccess("Password updated! Redirecting to login...");
      setTimeout(() => {
        window.location.hash = "";
        setView(VIEW.LOGIN);
      }, 2000);
    }
    setLoading(false);
  };

  // ── Google login ──────────────────────────────────────────────────────────
  const handleGoogle = async () => {
    setLoading(true); clear();
    const { error } = await supabase.auth.signInWithOAuth({
      provider: "google",
      options: { redirectTo: window.location.origin },
    });
    if (error) { setError(error.message); setLoading(false); }
  };

  // ── Shared header ─────────────────────────────────────────────────────────
  const Header = ({ title, subtitle }) => (
    <>
      <div style={S.eyebrow}>Secure multi-admin access</div>
      <div style={S.logo}>🛍️</div>
      <div style={S.title}>{title}</div>
      <div style={S.subtitle}>{subtitle}</div>
      <div style={S.metaRow}>
        <span style={S.metaPill}>Role-based onboarding</span>
        <span style={S.metaPill}>Admin team isolation</span>
        <span style={S.metaPill}>Store-level access control</span>
      </div>
    </>
  );

  // ── Render ────────────────────────────────────────────────────────────────
  return (
    <div style={S.page}>
      <div style={S.card}>

        {/* ── LOGIN ── */}
        {view === VIEW.LOGIN && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleLogin();
            }}
          >
            <Header title="Welcome back" subtitle="Sign in to your ShopManager account" />
            {error   && <div style={S.error}>❌ {error}</div>}
            {success && <div style={S.success}>✅ {success}</div>}

            {/* Google login */}
            <button style={S.googleBtn} onClick={handleGoogle} disabled={loading}
              onMouseEnter={e => e.currentTarget.style.borderColor = "#3B82F6"}
              onMouseLeave={e => e.currentTarget.style.borderColor = "#2a2a3d"}
            >
              <GoogleIcon /> Continue with Google
            </button>

            <div style={S.divider}>
              <div style={S.dividerLine}/> or <div style={S.dividerLine}/>
            </div>

            <Input label="Email" type="email" value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com" autoComplete="email" />
            <Input label="Password" type="password" value={password}
              onChange={e => setPassword(e.target.value)}
              placeholder="••••••••" autoComplete="current-password" />

            <div style={{ textAlign: "right", marginBottom: 16, marginTop: -8 }}>
              <button style={S.link} onClick={() => { clear(); setView(VIEW.FORGOT); }}>
                Forgot password?
              </button>
            </div>

            <button type="submit" style={{ ...S.btn, ...(loading ? S.btnDisabled : {}) }}
              disabled={loading}>
              {loading ? <Spin /> : "Sign in"}
            </button>

            <div style={{ textAlign: "center", marginTop: 20, fontSize: 13, color: "#666" }}>
              Don't have an account?{" "}
              <button style={S.link} onClick={() => { clear(); setView(VIEW.SIGNUP); }}>
                Sign up
              </button>
            </div>
          </form>
        )}

        {/* ── SIGNUP ── */}
        {view === VIEW.SIGNUP && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleSignup();
            }}
          >
            <Header title="Create account" subtitle="Join ShopManager to manage your Shopify store" />
            {error && <div style={S.error}>❌ {error}</div>}

            {/* Google signup */}
            <button style={S.googleBtn} onClick={handleGoogle} disabled={loading}
              onMouseEnter={e => e.currentTarget.style.borderColor = "#3B82F6"}
              onMouseLeave={e => e.currentTarget.style.borderColor = "#2a2a3d"}
            >
              <GoogleIcon /> Continue with Google
            </button>

            <div style={S.divider}>
              <div style={S.dividerLine}/> or <div style={S.dividerLine}/>
            </div>

            <Input label="Full Name" value={fullName}
              onChange={e => setFullName(e.target.value)}
              placeholder="John Doe" autoComplete="name" />
            <Input label="Email" type="email" value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com" autoComplete="email" />
            <Input label="Password" type="password" value={password}
              onChange={e => setPassword(e.target.value)}
              placeholder="Min 6 characters" autoComplete="new-password" />
            <Input label="Confirm Password" type="password" value={confirm}
              onChange={e => setConfirm(e.target.value)}
              placeholder="Repeat password" autoComplete="new-password" />

            <div style={{ marginBottom: 16 }}>
              <label style={S.label}>Select Role</label>
              <select
                value={signupRole}
                onChange={e => setSignupRole(e.target.value)}
                style={{ ...S.input, cursor: "pointer" }}
              >
                <option value="admin">Admin</option>
                <option value="manager">Manager</option>
                <option value="junior">Junior</option>
              </select>
            </div>

            {(signupRole === "manager" || signupRole === "junior") && (
              <Input
                label="Admin Email"
                type="email"
                value={adminEmail}
                onChange={e => setAdminEmail(e.target.value)}
                placeholder="admin@company.com"
                autoComplete="email"
              />
            )}

            <button type="submit" style={{ ...S.btn, ...(loading ? S.btnDisabled : {}) }}
              disabled={loading}>
              {loading ? <Spin /> : "Create account"}
            </button>

            <div style={{ textAlign: "center", marginTop: 20, fontSize: 13, color: "#666" }}>
              Already have an account?{" "}
              <button style={S.link} onClick={() => { clear(); setView(VIEW.LOGIN); }}>
                Sign in
              </button>
            </div>
          </form>
        )}

        {/* ── FORGOT PASSWORD ── */}
        {view === VIEW.FORGOT && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleForgot();
            }}
          >
            <Header title="Reset password" subtitle="Enter your email and we'll send you a reset link" />
            {error   && <div style={S.error}>❌ {error}</div>}
            {success && <div style={S.success}>✅ {success}</div>}

            <Input label="Email" type="email" value={email}
              onChange={e => setEmail(e.target.value)}
              placeholder="you@example.com" autoComplete="email" />

            <button type="submit" style={{ ...S.btn, ...(loading ? S.btnDisabled : {}) }}
              disabled={loading}>
              {loading ? <Spin /> : "Send reset link"}
            </button>

            <button style={S.btnSecondary}
              onClick={() => { clear(); setView(VIEW.LOGIN); }}>
              ← Back to login
            </button>
          </form>
        )}

        {/* ── RESET PASSWORD (from email link) ── */}
        {view === VIEW.RESET && (
          <form
            onSubmit={(e) => {
              e.preventDefault();
              handleReset();
            }}
          >
            <Header title="Set new password" subtitle="Choose a strong password for your account" />
            {error   && <div style={S.error}>❌ {error}</div>}
            {success && <div style={S.success}>✅ {success}</div>}

            <Input label="New Password" type="password" value={newPass}
              onChange={e => setNewPass(e.target.value)}
              placeholder="Min 6 characters" autoComplete="new-password" />
            <Input label="Confirm New Password" type="password" value={newConf}
              onChange={e => setNewConf(e.target.value)}
              placeholder="Repeat new password" autoComplete="new-password" />

            <button type="submit" style={{ ...S.btn, ...(loading ? S.btnDisabled : {}) }}
              disabled={loading}>
              {loading ? <Spin /> : "Update password"}
            </button>
          </form>
        )}

        {/* ── EMAIL PENDING CONFIRMATION ── */}
        {view === VIEW.PENDING && (
          <>
            <div style={{ textAlign: "center" }}>
              <div style={{ fontSize: 48, marginBottom: 16 }}>📧</div>
              <div style={S.title}>Check your email</div>
              <div style={{ ...S.subtitle, marginBottom: 24 }}>
                We sent a confirmation link to<br />
                <strong style={{ color: "#e8e8f0" }}>{email}</strong>
              </div>
              <div style={{ fontSize: 12, color: "#555", marginBottom: 24 }}>
                Click the link in the email to activate your account.
                The link expires in 24 hours.
              </div>
              <button style={S.btnSecondary}
                onClick={() => { clear(); setView(VIEW.LOGIN); }}>
                Back to login
              </button>
            </div>
          </>
        )}

      </div>
    </div>
  );
}
