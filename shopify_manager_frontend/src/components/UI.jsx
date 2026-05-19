import { createPortal } from "react-dom";
import { Ico } from "./Icons";

export const Badge = ({ status }) => {
  const styles = {
    active: {
      bg: "var(--success-light)",
      color: "var(--success)",
      border: "var(--success-border)",
    },
    draft: {
      bg: "var(--warning-light)",
      color: "var(--warning)",
      border: "var(--warning-border)",
    },
    archived: {
      bg: "var(--bg-overlay-light)",
      color: "var(--gray-color)",
      border: "var(--gray-border)",
    },
  };
  const s = styles[status] || styles.draft;
  return (
    <span
      style={{
        padding: "3px 7.5px",
        borderRadius: 20,
        fontSize: 8.25,
        fontWeight: 600,
        background: s.bg,
        color: s.color,
        border: `1px solid ${s.border}`,
        textTransform: "uppercase",
        letterSpacing: "0.5px",
        display: "inline-block",
      }}
    >
      {status || "draft"}
    </span>
  );
};

export const Modal = ({ title, subtitle, onClose, children, maxWidth = 660 }) =>
  createPortal(
    <div
      className="modal-overlay"
      onClick={(e) => e.target === e.currentTarget && onClose()}
    >
      <div className="modal-box" style={{ maxWidth }}>
        <div
          style={{
            padding: "24px 28px 16px",
            borderBottom: "1px solid var(--border-color)",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            gap: 16,
          }}
        >
          <div>
            <h2
              style={{
                fontFamily: "'Syne', sans-serif",
                fontSize: 16.5,
                fontWeight: 700,
                color: "#fff",
                lineHeight: 1.2,
              }}
            >
              {title}
            </h2>
            {subtitle && (
              <p
                style={{
                  fontSize: 13,
                  color: "var(--text-muted)",
                  marginTop: 4,
                }}
              >
                {subtitle}
              </p>
            )}
          </div>
          <button
            onClick={onClose}
            className="btn btn-secondary"
            style={{ padding: 6, borderRadius: 10 }}
            aria-label="Close"
          >
            <Ico n="x" size={18} />
          </button>
        </div>
        <div style={{ padding: "18px 21px 21px" }}>{children}</div>
      </div>
    </div>,
    document.body,
  );

export const PageLoadingOverlay = ({
  badge = "WORKING",
  title = "Please wait",
  subtitle = "We are processing your request.",
}) => (
  <div
    style={{
      position: "absolute",
      inset: 0,
      zIndex: 80,
      background: "var(--bg-overlay)",
      backdropFilter: "blur(6px)",
      display: "flex",
      alignItems: "flex-start",
      justifyContent: "center",
      padding: "56px 16px 16px",
    }}
  >
    <div
      className="card"
      style={{
        width: "min(92vw, 270px)",
        padding: 18,
        textAlign: "center",
        border: "1px solid var(--border-strong)",
        boxShadow: "var(--shadow-xl)",
      }}
    >
      <div
        style={{
          display: "inline-flex",
          alignItems: "center",
          justifyContent: "center",
          padding: "3px 7.5px",
          borderRadius: 999,
          background: "var(--warning-light)",
          color: "var(--warning)",
          fontSize: 8.25,
          fontWeight: 700,
          letterSpacing: "0.06em",
          marginBottom: 10.5,
        }}
      >
        {badge}
      </div>
      <div
        style={{
          width: 39,
          height: 39,
          margin: "0 auto 10.5px",
          borderRadius: 12,
          background: "var(--bg-overlay-light)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--accent)",
        }}
      >
        <Ico n="sync" size={18} className="animate-spin" />
      </div>
      <div
        style={{
          color: "var(--text-primary)",
          fontSize: 13.5,
          fontWeight: 700,
          marginBottom: 4.5,
          fontFamily: "'Syne', sans-serif",
        }}
      >
        {title}
      </div>
      <div
        style={{
          color: "var(--text-muted)",
          fontSize: 9.75,
          lineHeight: 1.5,
        }}
      >
        {subtitle}
      </div>
    </div>
  </div>
);

export const Field = ({
  label,
  value,
  onChange,
  type = "text",
  options,
  rows,
  pre,
  suf,
}) => (
  <div style={{ display: "flex", flexDirection: "column", gap: 4.5 }}>
    <label
      style={{
        fontSize: 9,
        fontWeight: 600,
        color: "var(--text-secondary)",
        textTransform: "uppercase",
        letterSpacing: "0.6px",
      }}
    >
      {label}
    </label>
    <div
      style={{ position: "relative", display: "flex", alignItems: "center" }}
    >
      {pre && (
        <span
          style={{
            position: "absolute",
            left: 12,
            color: "var(--text-muted)",
            fontSize: 10.5,
            pointerEvents: "none",
            zIndex: 1,
          }}
        >
          {pre}
        </span>
      )}
      {options ? (
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="field-input"
          style={{
            padding: "7.5px 10.5px",
            appearance: "none",
            cursor: "pointer",
          }}
        >
          {options.map((o) => (
            <option key={o.v || o} value={o.v || o}>
              {o.l || o}
            </option>
          ))}
        </select>
      ) : type === "textarea" ? (
        <textarea
          value={value}
          onChange={(e) => onChange(e.target.value)}
          rows={rows || 3}
          className="field-input"
          style={{ resize: "vertical" }}
        />
      ) : (
        <input
          type={type}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="field-input"
          style={{
            padding: `10px ${suf ? "40px" : "14px"} 10px ${pre ? "36px" : "14px"}`,
          }}
        />
      )}
      {suf && (
        <span
          style={{
            position: "absolute",
            right: 12,
            color: "var(--text-muted)",
            fontSize: 14,
            pointerEvents: "none",
          }}
        >
          {suf}
        </span>
      )}
    </div>
  </div>
);
