import { Ico } from "./Icons";

export default function AccessDenied() {
  return (
    <div style={{ minHeight: "100vh", background: "var(--bg-primary)", display: "grid", placeItems: "center", padding: "20px" }}>
      <div style={{ maxWidth: "480px", width: "100%", background: "var(--bg-card)", border: "1px solid var(--danger-border)", borderRadius: "12px", padding: "40px", textAlign: "center" }}>
        <Ico n="lock" size={48} color="var(--danger)" style={{ marginBottom: "16px" }} />
        <h1 style={{ fontSize: "20px", fontWeight: 700, color: "var(--text-primary)", margin: "0 0 8px 0" }}>Access Denied</h1>
        <p style={{ fontSize: "14px", color: "var(--text-muted)", margin: "0" }}>You don't have permission to access this page. Contact your administrator.</p>
      </div>
    </div>
  );
}
