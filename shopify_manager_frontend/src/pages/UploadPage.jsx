import { useState } from "react";
import { api } from "../api/api";
import { Spin } from "../components/Icons";

const UploadPage = ({ toast }) => {
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [full, setFull] = useState(null);
  const [edited, setEdited] = useState({});
  const [loading, setLoading] = useState(false);
  const [pushing, setPushing] = useState(false);
  const [results, setResults] = useState(null);
  const [drag, setDrag] = useState(false);
  const [step, setStep] = useState(1);

  const drop = (f) => {
    setFile(f);
    setPreview(null);
    setFull(null);
    setResults(null);
    setStep(1);
  };
  const doPreview = async () => {
    setLoading(true);
    try {
      const fd = new FormData();
      fd.append("file", file);
      const d = await api.upload("/upload/preview", fd);
      if (d.columns) {
        setPreview(d);
        setStep(2);
      } else toast(d.detail || "Failed", "error");
    } catch (err) {
      toast(err.message || "Preview failed", "error");
    } finally {
      setLoading(false);
    }
  };
  const doParse = async () => {
    setLoading(true);
    try {
      const fd = new FormData();
      fd.append("file", file);
      const d = await api.upload("/upload/parse", fd);
      if (d.columns) {
        setFull(d);
        setEdited({});
        setStep(3);
      } else toast(d.detail || "Failed", "error");
    } catch (err) {
      toast(err.message || "Parse failed", "error");
    } finally {
      setLoading(false);
    }
  };
  const doPush = async () => {
    if (!full || !window.confirm(`Push ${full.total_rows} rows to Shopify?`))
      return;
    setPushing(true);
    try {
      const prods = full.data.map((r, i) => ({ ...r, ...edited[i] }));
      const d = await api.post("/upload/push-to-shopify", prods);
      setResults(d);
      toast(`${d.success?.length || 0} products pushed!`);
      setStep(4);
    } catch (err) {
      toast(err.message || "Push failed", "error");
    } finally {
      setPushing(false);
    }
  };
  const steps = ["Select File", "Preview", "Edit & Review", "Done"];

  return (
    <div
      className="fade-up container max-w-7xl mx-auto px-4 pt-4"
      style={{ position: "relative", minHeight: "calc(100vh - 120px)" }}
    >
      <div style={{ marginBottom: 21 }}>
        <h1
          style={{
            fontFamily: "'Syne', sans-serif",
            fontSize: 24,
            fontWeight: 800,
            color: "var(--text-primary)",
          }}
        >
          Upload Products
        </h1>
        <p
          style={{ color: "var(--text-muted)", marginTop: 3, fontSize: 11.25 }}
        >
          Import Excel or CSV and push to Shopify
        </p>
      </div>

      {/* Stepper */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          marginBottom: 24,
          gap: 0,
        }}
      >
        {steps.map((s, i) => (
          <div
            key={s}
            style={{
              display: "flex",
              alignItems: "center",
              flex: i < steps.length - 1 ? 1 : "none",
            }}
          >
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div
                style={{
                  width: 30,
                  height: 30,
                  borderRadius: "50%",
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  background:
                    step > i + 1
                      ? "var(--accent)"
                      : step === i + 1
                        ? "var(--accent-gradient)"
                        : "var(--bg-card)",
                  border: `2px solid ${step >= i + 1 ? "var(--accent)" : "var(--border-light)"}`,
                  fontSize: 9,
                  fontWeight: 700,
                  color: step >= i + 1 ? "#fff" : "var(--text-muted)",
                  transition: "var(--transition)",
                }}
              >
                {step > i + 1 ? "✓" : i + 1}
              </div>
              <span
                style={{
                  fontSize: 9.75,
                  fontWeight: 500,
                  color:
                    step === i + 1
                      ? "var(--accent)"
                      : step > i + 1
                        ? "var(--text-primary)"
                        : "var(--text-muted)",
                }}
              >
                {s}
              </span>
            </div>
            {i < steps.length - 1 && (
              <div
                style={{
                  flex: 1,
                  height: 2,
                  background:
                    step > i + 1 ? "var(--accent)" : "var(--border-color)",
                  margin: "0 9px",
                  transition: "background 0.3s",
                }}
              />
            )}
          </div>
        ))}
      </div>

      {/* Dropzone */}
      <div
        style={{
          background: "var(--bg-card)",
          border: "1px solid var(--border-color)",
          borderRadius: 24,
          padding: 21,
          marginBottom: 15,
        }}
      >
        <div
          onDragOver={(e) => {
            e.preventDefault();
            setDrag(true);
          }}
          onDragLeave={() => setDrag(false)}
          onDrop={(e) => {
            e.preventDefault();
            setDrag(false);
            const f = e.dataTransfer.files[0];
            if (f) drop(f);
          }}
          onClick={() => document.getElementById("fileInput").click()}
          style={{
            border: `2px dashed ${drag ? "var(--accent)" : file ? "var(--success)" : "var(--border-light)"}`,
            borderRadius: 16,
            padding: "36px 18px",
            textAlign: "center",
            cursor: "pointer",
            background: drag
              ? "var(--accent-subtle)"
              : file
                ? "var(--success-subtle)"
                : "transparent",
            transition: "var(--transition)",
          }}
        >
          <input
            id="fileInput"
            type="file"
            accept=".xlsx,.xls,.csv"
            style={{ display: "none" }}
            onChange={(e) => e.target.files[0] && drop(e.target.files[0])}
          />
          <div style={{ fontSize: 30, marginBottom: 9 }}>📊</div>
          <p
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: file ? "var(--success)" : "var(--text-primary)",
              marginBottom: 3,
            }}
          >
            {file ? `✓ ${file.name}` : "Drop file here or click to browse"}
          </p>
          <p style={{ fontSize: 9.75, color: "var(--text-muted)" }}>
            Supports .xlsx, .xls, .csv
          </p>
        </div>
        {file && (
          <div style={{ display: "flex", gap: 9, marginTop: 15 }}>
            <button
              onClick={doPreview}
              disabled={loading}
              className="btn btn-secondary"
              style={{ flex: 1, padding: "12px" }}
            >
              {loading && step === 1 ? <Spin size={16} /> : null} Preview (10
              rows)
            </button>
            <button
              onClick={doParse}
              disabled={loading}
              className="btn btn-primary"
              style={{ flex: 1, padding: "12px" }}
            >
              {loading ? <Spin size={16} /> : null} Parse Full Data
            </button>
          </div>
        )}
      </div>

      {/* Preview */}
      {preview && !full && (
        <div
          style={{
            background: "var(--bg-card)",
            border: "1px solid var(--border-color)",
            borderRadius: 24,
            padding: 24,
            marginBottom: 20,
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 16,
            }}
          >
            <h3
              style={{
                fontFamily: "'Syne', sans-serif",
                fontSize: 18,
                fontWeight: 700,
                color: "#fff",
              }}
            >
              Preview
            </h3>
            <span style={{ fontSize: 13, color: "var(--text-muted)" }}>
              {preview.total_rows} rows · first 10 shown
            </span>
          </div>
          <div style={{ overflowX: "auto" }}>
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontSize: 13,
              }}
            >
              <thead>
                <tr style={{ borderBottom: "1px solid var(--border-color)" }}>
                  {preview.columns.map((c) => (
                    <th
                      key={c}
                      style={{
                        padding: "10px 12px",
                        textAlign: "left",
                        color: "var(--text-muted)",
                        fontWeight: 600,
                        fontSize: 11,
                        textTransform: "uppercase",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {(preview.preview || preview.data || []).map((row, i) => (
                  <tr
                    key={i}
                    style={{ borderBottom: "1px solid var(--border-color)" }}
                  >
                    {preview.columns.map((c) => (
                      <td
                        key={c}
                        style={{
                          padding: "10px 12px",
                          color: "var(--text-secondary)",
                          maxWidth: 160,
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          whiteSpace: "nowrap",
                        }}
                      >
                        {String(row[c] || "")}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Edit & Review */}
      {full && !results && (
        <div
          style={{
            background: "var(--bg-card)",
            border: "1px solid var(--border-color)",
            borderRadius: 24,
            padding: 24,
            marginBottom: 20,
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              marginBottom: 16,
            }}
          >
            <h3
              style={{
                fontFamily: "'Syne', sans-serif",
                fontSize: 18,
                fontWeight: 700,
                color: "#fff",
              }}
            >
              Edit & Review
            </h3>
            <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
              <span style={{ fontSize: 13, color: "var(--text-muted)" }}>
                {full.total_rows} rows
              </span>
              <button
                onClick={doPush}
                disabled={pushing}
                className="btn btn-primary"
                style={{ padding: "10px 18px" }}
              >
                {pushing ? (
                  <>
                    <Spin size={16} /> Pushing...
                  </>
                ) : (
                  "🚀 Push to Shopify"
                )}
              </button>
            </div>
          </div>
          <div
            style={{
              overflowX: "auto",
              maxHeight: 500,
              overflowY: "auto",
              border: "1px solid var(--border-color)",
              borderRadius: 12,
            }}
          >
            <table
              style={{
                width: "100%",
                borderCollapse: "collapse",
                fontSize: 12,
              }}
            >
              <thead
                style={{
                  position: "sticky",
                  top: 0,
                  background: "var(--bg-card)",
                  zIndex: 1,
                }}
              >
                <tr style={{ borderBottom: "1px solid var(--border-color)" }}>
                  <th
                    style={{
                      padding: "10px 12px",
                      color: "var(--text-muted)",
                      fontSize: 11,
                      fontWeight: 600,
                      textTransform: "uppercase",
                    }}
                  >
                    #
                  </th>
                  {full.columns.map((c) => (
                    <th
                      key={c}
                      style={{
                        padding: "10px 12px",
                        textAlign: "left",
                        color: "var(--text-muted)",
                        fontSize: 11,
                        fontWeight: 600,
                        textTransform: "uppercase",
                        whiteSpace: "nowrap",
                      }}
                    >
                      {c}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {full.data.map((row, ri) => (
                  <tr
                    key={ri}
                    style={{ borderBottom: "1px solid var(--border-color)" }}
                  >
                    <td
                      style={{
                        padding: "6px 12px",
                        color: "var(--text-muted)",
                        textAlign: "center",
                      }}
                    >
                      {ri + 1}
                    </td>
                    {full.columns.map((c) => (
                      <td key={c} style={{ padding: "4px 8px" }}>
                        <input
                          value={edited[ri]?.[c] ?? String(row[c] || "")}
                          onChange={(e) =>
                            setEdited((p) => ({
                              ...p,
                              [ri]: { ...p[ri], [c]: e.target.value },
                            }))
                          }
                          className="field-input"
                          style={{
                            padding: "6px 8px",
                            fontSize: 11,
                            minWidth: 80,
                          }}
                        />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* Results */}
      {results && (
        <div
          className="fade-up"
          style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}
        >
          <div
            style={{
              background: "var(--success-bg)",
              border: "1px solid var(--success-border)",
              borderRadius: 20,
              padding: 28,
            }}
          >
            <div
              style={{
                fontSize: 42,
                fontWeight: 800,
                color: "var(--success)",
                fontFamily: "'Syne', sans-serif",
                lineHeight: 1,
              }}
            >
              {results.success?.length}
            </div>
            <div
              style={{ fontSize: 15, color: "var(--success)", marginTop: 6 }}
            >
              Created successfully
            </div>
          </div>
          <div
            style={{
              background: "var(--danger-bg)",
              border: "1px solid var(--danger-border)",
              borderRadius: 20,
              padding: 28,
            }}
          >
            <div
              style={{
                fontSize: 42,
                fontWeight: 800,
                color: "var(--danger)",
                fontFamily: "'Syne', sans-serif",
                lineHeight: 1,
              }}
            >
              {results.errors?.length}
            </div>
            <div style={{ fontSize: 15, color: "var(--danger)", marginTop: 6 }}>
              Failed
            </div>
          </div>
          <div style={{ gridColumn: "1/-1" }}>
            <button
              onClick={() => {
                setFile(null);
                setPreview(null);
                setFull(null);
                setResults(null);
                setStep(1);
              }}
              className="btn btn-primary"
              style={{ padding: "14px 24px", fontSize: 15 }}
            >
              Upload Another
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export default UploadPage;
