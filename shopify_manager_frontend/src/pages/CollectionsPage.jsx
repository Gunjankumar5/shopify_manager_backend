import { useState, useEffect, useCallback } from "react";
import { API_BASE_URL } from "../api/config";
import { Ico, Spin } from "../components/Icons";
import AddCollectionPage from "./AddcollectionPage";
import { api } from "../api/api";

export default function CollectionsPage({ toast }) {
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  // null = list, true = new, object = edit context
  const [formMode, setFormMode] = useState(null);

  // Helper to save formMode to localStorage (store only ID or "new")
  const saveFormMode = useCallback((mode) => {
    try {
      if (mode === null) {
        localStorage.removeItem("collectionFormMode");
      } else if (mode === true) {
        localStorage.setItem("collectionFormMode", JSON.stringify("new"));
      } else if (mode?.id) {
        localStorage.setItem("collectionFormMode", JSON.stringify(mode.id));
      }
    } catch {
      // Silently fail if localStorage is unavailable
    }
  }, []);

  // Restore formMode from localStorage
  useEffect(() => {
    try {
      const saved = localStorage.getItem("collectionFormMode");
      if (!saved) return;

      const parsed = JSON.parse(saved);
      if (parsed === "new") {
        setFormMode(true);
      } else if (typeof parsed === "string" && collections.length > 0) {
        // It's a collection ID - only restore if we have collections loaded
        const collection = collections.find((c) => c.id === parsed);
        if (collection) {
          setFormMode(collection);
        } else if (!loading) {
          // Collections loaded but this ID not found - clear it
          localStorage.removeItem("collectionFormMode");
        }
      }
    } catch {
      // Silently fail if there's an error
    }
  }, [collections, loading]);

  // Save formMode whenever it changes
  useEffect(() => {
    saveFormMode(formMode);
  }, [formMode, saveFormMode]);

  // On first mount, try to restore form mode immediately (for "new" forms)
  useEffect(() => {
    try {
      const saved = localStorage.getItem("collectionFormMode");
      if (!saved) return;
      const parsed = JSON.parse(saved);
      if (parsed === "new") {
        setFormMode(true);
      }
    } catch {
      // Silently fail
    }
  }, []);

  useEffect(() => {
    fetchCollections();
  }, []);

  async function fetchCollections(force = false) {
    try {
      setLoading(true);
      const data = await api.get(`/collections/`, { ttlMs: 300000, force });
      setCollections(data.custom_collections || []);
    } catch (e) {
      toast?.(e.message, "error");
    } finally {
      setLoading(false);
    }
  }

  async function handleSync() {
    setRefreshing(true);
    await fetchCollections(true);
    setRefreshing(false);
  }

  // ── If form open, render full-page editor ───────────────────────────────
  if (formMode !== null) {
    return (
      <AddCollectionPage
        key={
          formMode === true
            ? "new-collection"
            : `edit-collection-${formMode.id}`
        }
        toast={toast}
        editCollection={formMode === true ? null : formMode}
        onBack={() => {
          setFormMode(null);
          localStorage.removeItem("collectionFormMode");
          fetchCollections();
        }}
      />
    );
  }

  const getImageSrc = (c) => c?.image?.src || "";

  const TYPE_COLORS = {
    custom: { bg: "var(--info-light)", color: "var(--color-blue)" },
    smart: { bg: "var(--color-purple-light)", color: "var(--color-purple)" },
  };

  return (
    <div
      className="fade-up container max-w-5xl mx-auto px-4 py-8"
      style={{ position: "relative", minHeight: "calc(100vh - 120px)" }}
    >
      {/* Header */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          marginBottom: 18,
          gap: 9,
          flexWrap: "wrap",
        }}
      >
        <div>
          <h1 className="font-display text-2xl font-bold text-primary flex items-center gap-2">
            <Ico n="collections" size="lg" /> Collections
          </h1>
          <p className="text-muted text-sm mt-0.5">
            Organize products into custom collections
          </p>
        </div>
        <div style={{ display: "flex", gap: 6 }}>
          <button
            onClick={handleSync}
            disabled={loading || refreshing}
            className="btn btn-secondary"
          >
            {refreshing ? <Spin size={14} /> : <Ico n="sync" size={14} />}
            <span>{refreshing ? "Syncing…" : "Sync"}</span>
          </button>
          <button
            onClick={() => setFormMode(true)}
            className="btn btn-primary flex items-center gap-2"
            style={{
              background: "var(--accent-gradient)",
              boxShadow: "var(--shadow-accent)",
            }}
          >
            <Ico n="plus" size={14} />
            New Collection
          </button>
        </div>
      </div>

      {/* Stats bar */}
      {!loading && collections.length > 0 && (
        <div
          style={{
            display: "flex",
            gap: 6,
            marginBottom: 15,
            flexWrap: "wrap",
          }}
        >
          {[
            { label: "Total", value: collections.length, color: "accent" },
            {
              label: "Custom",
              value: collections.filter(
                (c) => c.collection_type === "custom" || !c.collection_type,
              ).length,
              color: "accent",
            },
            {
              label: "Smart",
              value: collections.filter((c) => c.collection_type === "smart")
                .length,
              color: "warning",
            },
          ].map((s) => (
            <div
              key={s.label}
              className="card flex items-center gap-2 p-2"
              style={{ minWidth: 90 }}
            >
              <div>
                <div
                  className={`font-display text-lg font-bold text-${s.color}`}
                >
                  {s.value}
                </div>
                <div className="text-muted text-xs">{s.label}</div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Loading skeleton */}
      {loading && (
        <div style={{ display: "grid", gap: 9 }}>
          {[...Array(3)].map((_, i) => (
            <div
              key={i}
              className="skeleton"
              style={{ height: 110, borderRadius: 10 }}
            />
          ))}
        </div>
      )}

      {/* Empty state */}
      {!loading && collections.length === 0 && (
        <div className="card p-12 text-center">
          <Ico n="collections" size="xl" className="text-muted mb-3" />
          <p className="text-secondary text-sm mt-3">
            No collections found. Create one to get started!
          </p>
          <button
            onClick={() => setFormMode(true)}
            className="btn btn-primary mt-4 mx-auto"
            style={{ background: "var(--accent-gradient)" }}
          >
            <Ico n="plus" size={14} /> New Collection
          </button>
        </div>
      )}

      {/* Collections list */}
      {!loading && collections.length > 0 && (
        <div style={{ display: "grid", gap: 7.5 }}>
          {collections.map((col) => {
            const imgSrc = getImageSrc(col);
            const type = col.collection_type || "custom";
            const tc = TYPE_COLORS[type] || TYPE_COLORS.custom;
            return (
              <div
                key={col.id}
                className="card card-hover"
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 16,
                  padding: "14px 18px",
                  cursor: "pointer",
                  transition: "all .15s",
                }}
                onClick={() =>
                  setFormMode({
                    id: col.id,
                    collection_type: col.collection_type || "custom",
                    title: col.title,
                    body_html: col.body_html,
                    image: col.image,
                    handle: col.handle,
                    published_at: col.published_at,
                  })
                }
              >
                {/* Thumbnail */}
                <div
                  style={{
                    width: 60,
                    height: 60,
                    borderRadius: 8,
                    border: "1px solid var(--border-strong)",
                    overflow: "hidden",
                    flexShrink: 0,
                    background: "var(--bg-secondary)",
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {imgSrc ? (
                    <img
                      src={imgSrc}
                      alt={col.title}
                      style={{
                        width: "100%",
                        height: "100%",
                        objectFit: "cover",
                      }}
                    />
                  ) : (
                    <Ico n="image" size={20} className="text-muted" />
                  )}
                </div>

                {/* Info */}
                <div style={{ flex: 1, minWidth: 0 }}>
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 8,
                      marginBottom: 2.25,
                    }}
                  >
                    <span
                      className="font-display font-bold text-primary"
                      style={{ fontSize: 10.5 }}
                    >
                      {col.title}
                    </span>
                    <span
                      style={{
                        fontSize: 10,
                        fontWeight: 700,
                        padding: "2px 7px",
                        borderRadius: 4,
                        background: tc.bg,
                        color: tc.color,
                        textTransform: "uppercase",
                        letterSpacing: ".04em",
                      }}
                    >
                      {type}
                    </span>
                  </div>
                  {col.body_html && (
                    <div
                      className="text-muted"
                      style={{
                        fontSize: 12,
                        overflow: "hidden",
                        display: "-webkit-box",
                        WebkitLineClamp: 1,
                        WebkitBoxOrient: "vertical",
                      }}
                      dangerouslySetInnerHTML={{ __html: col.body_html }}
                    />
                  )}
                  <div
                    className="text-muted"
                    style={{ fontSize: 8.25, marginTop: 2.25 }}
                  >
                    ID:{" "}
                    <code
                      style={{
                        fontSize: 10,
                        background: "var(--bg-secondary)",
                        padding: "1px 5px",
                        borderRadius: 3,
                      }}
                    >
                      {col.id}
                    </code>
                  </div>
                </div>

                {/* Arrow */}
                <div
                  style={{
                    flexShrink: 0,
                    color: "var(--text-muted)",
                    opacity: 0.4,
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
                    <path d="M9 18l6-6-6-6" />
                  </svg>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
