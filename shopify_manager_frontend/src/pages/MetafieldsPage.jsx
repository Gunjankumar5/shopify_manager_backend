import { useState, useEffect, useCallback } from "react";
import { API_BASE_URL } from "../api/config";
import { authFetch } from "../lib/authFetch";
import { Spin } from "../components/Icons";
import MetafieldsPanel from "../components/MetafieldsPanel";

const C = {
  card: "var(--bg-card)",
  bgCard: "var(--bg-card)",
  bgInput: "var(--bg-input)",
  bgSecondary: "var(--bg-secondary)",
  bgPrimary: "var(--bg-primary)",
  border: "var(--border-strong)",
  borderFocus: "var(--accent)",
  text: "var(--text-primary)",
  muted: "var(--text-muted)",
  disabled: "var(--text-muted)",
  accent: "var(--accent)",
  success: "var(--success)",
  danger: "var(--danger)",
};

const TABS = [
  { id: "products", label: "Products", emoji: "📦" },
  { id: "collections", label: "Collections", emoji: "📂" },
  { id: "variants", label: "Variants", emoji: "🎨" },
];

const MODES = [
  { id: "browse", label: "Browse", emoji: "📋" },
  { id: "definitions", label: "Definitions", emoji: "⚙️" },
];

export default function MetafieldsPage({ toast, activeStore }) {
  const [mode, setMode] = useState("browse");
  const [tab, setTab] = useState("products");
  const [resources, setResources] = useState([]);
  const [selectedResource, setSelectedResource] = useState(null);
  const [selectedVariant, setSelectedVariant] = useState(null);
  const [variants, setVariants] = useState([]);
  const [loading, setLoading] = useState(false);
  const [search, setSearch] = useState("");
  const [definitions, setDefinitions] = useState({
    products: [],
    collections: [],
    variants: [],
  });
  const [expandedSections, setExpandedSections] = useState({
    products: true,
    collections: true,
    variants: true,
  });
  const [selectedDefinition, setSelectedDefinition] = useState(null);

  // ── Fetch resource list when tab changes ─────────────────────────────────
  const fetchResources = useCallback(async () => {
    setLoading(true);
    setResources([]);
    setSelectedResource(null);
    setSelectedVariant(null);
    setSearch("");
    try {
      if (tab === "products" || tab === "variants") {
        const res = await authFetch(`${API_BASE_URL}/products/all`);
        const text = await res.text();
        const lines = text.trim().split("\n");
        const products = [];
        for (const line of lines) {
          try {
            const obj = JSON.parse(line);
            if (obj.type === "page" && obj.products) {
              products.push(...obj.products);
            }
          } catch (e) {
            // Skip invalid lines
          }
        }
        setResources(products);
      } else {
        const res = await authFetch(`${API_BASE_URL}/collections/`);
        const data = await res.json();
        setResources(data.custom_collections || []);
      }
    } catch (err) {
      console.error("Failed to load resources:", err);
      toast?.("Failed to load resources", "error");
    }
    setLoading(false);
  }, [tab, toast]);

  const fetchDefinitions = useCallback(async () => {
    setLoading(true);
    try {
      console.log("🔄 Fetching metafield definitions from:", API_BASE_URL);

      const responses = await Promise.all([
        authFetch(`${API_BASE_URL}/metafields/definitions/products`),
        authFetch(`${API_BASE_URL}/metafields/definitions/collections`),
        authFetch(`${API_BASE_URL}/metafields/definitions/variants`),
      ]);

      const [prodRes, collRes, varRes] = responses;

      console.log(
        "📦 Product response status:",
        prodRes.status,
        prodRes.statusText,
      );
      console.log(
        "📂 Collection response status:",
        collRes.status,
        collRes.statusText,
      );
      console.log(
        "🎨 Variant response status:",
        varRes.status,
        varRes.statusText,
      );

      if (!prodRes.ok) {
        const err = await prodRes.text();
        console.error("❌ Product definitions error:", err);
        throw new Error(`Product definitions error: ${prodRes.status} ${err}`);
      }
      if (!collRes.ok) {
        const err = await collRes.text();
        console.error("❌ Collection definitions error:", err);
        throw new Error(
          `Collection definitions error: ${collRes.status} ${err}`,
        );
      }
      if (!varRes.ok) {
        const err = await varRes.text();
        console.error("❌ Variant definitions error:", err);
        throw new Error(`Variant definitions error: ${varRes.status} ${err}`);
      }

      const prodDefs = await prodRes.json();
      const collDefs = await collRes.json();
      const variantDefs = await varRes.json();

      console.log(
        "✅ Product definitions loaded:",
        prodDefs.definitions?.length || 0,
      );
      console.log(
        "✅ Collection definitions loaded:",
        collDefs.definitions?.length || 0,
      );
      console.log(
        "✅ Variant definitions loaded:",
        variantDefs.definitions?.length || 0,
      );
      console.log("Full response - Products:", prodDefs);
      console.log("Full response - Collections:", collDefs);
      console.log("Full response - Variants:", variantDefs);

      setDefinitions({
        products: prodDefs.definitions || [],
        collections: collDefs.definitions || [],
        variants: variantDefs.definitions || [],
      });
      console.log("✅ Definitions state updated");
    } catch (err) {
      console.error("❌ Failed to load definitions:", err);
      toast?.("Failed to load definitions: " + err.message, "error");
    }
    setLoading(false);
  }, [toast]);

  useEffect(() => {
    if (mode === "definitions") {
      console.log("📋 Switching to Definitions mode - fetching...");
      setSelectedDefinition(null);
      fetchDefinitions();
    } else {
      console.log(
        "📦 Switching to Browse mode - fetching resources for tab:",
        tab,
      );
      fetchResources();
    }
  }, [mode, tab, fetchResources, fetchDefinitions]);

  // ── When a product is selected in Variants tab, expose its variants ────────
  useEffect(() => {
    if (tab === "variants" && selectedResource) {
      setVariants(selectedResource.variants || []);
      setSelectedVariant(null);
    }
  }, [selectedResource, tab]);

  const filtered = resources.filter((r) =>
    r.title?.toLowerCase().includes(search.toLowerCase()),
  );

  // What MetafieldsPanel should render
  const panelResource =
    tab === "variants"
      ? "variants"
      : tab === "collections"
        ? "collections"
        : "products";
  const panelId =
    tab === "variants" ? selectedVariant?.id : selectedResource?.id;
  const panelName =
    tab === "variants"
      ? selectedVariant
        ? `${selectedResource?.title} / ${selectedVariant?.title}`
        : null
      : selectedResource?.title;

  return (
    <div
      style={{
        display: "flex",
        height: "100vh",
        overflow: "hidden",
        fontFamily: "inherit",
      }}
    >
      {/* ── Left panel: resource selector ─────────────────────────────────── */}
      <div
        style={{
          width: 210,
          flexShrink: 0,
          borderRight: `1px solid ${C.border}`,
          display: "flex",
          flexDirection: "column",
          background: "var(--bg-secondary)",
        }}
      >
        {/* Page title */}
        <div
          style={{
            padding: "15px 12px 10.5px",
            borderBottom: `1px solid ${C.border}`,
          }}
        >
          <div
            style={{
              fontFamily: "'Syne', sans-serif",
              fontSize: 13.5,
              fontWeight: 800,
              color: C.text,
            }}
          >
            🏷️ Metafields
          </div>
          <div style={{ fontSize: 9, color: C.muted, marginTop: 2.25 }}>
            View and edit custom data
          </div>
        </div>

        {/* Mode selector */}
        <div
          style={{
            display: "flex",
            gap: 4.5,
            padding: "9px 12px",
            borderBottom: `1px solid ${C.border}`,
          }}
        >
          {MODES.map((m) => (
            <button
              key={m.id}
              onClick={() => setMode(m.id)}
              style={{
                flex: 1,
                padding: "6px 9px",
                background: mode === m.id ? C.accent : C.bgCard,
                border: `1px solid ${mode === m.id ? C.accent : C.border}`,
                borderRadius: 4.5,
                color: mode === m.id ? "#fff" : C.text,
                fontSize: 9,
                fontWeight: 600,
                cursor: "pointer",
                transition: "all 0.2s",
              }}
            >
              {m.emoji} {m.label}
            </button>
          ))}
        </div>

        {/* Tabs - only show in Browse mode */}
        {mode === "browse" && (
          <>
            <div
              style={{ display: "flex", borderBottom: `1px solid ${C.border}` }}
            >
              {TABS.map((t) => (
                <button
                  key={t.id}
                  onClick={() => setTab(t.id)}
                  style={{
                    flex: 1,
                    padding: "8.25px 3px",
                    background: "transparent",
                    border: "none",
                    borderBottom: `2px solid ${tab === t.id ? C.accent : "transparent"}`,
                    color: tab === t.id ? C.accent : C.muted,
                    fontSize: 8.25,
                    fontWeight: 600,
                    cursor: "pointer",
                    fontFamily: "inherit",
                    transition: "all .15s",
                  }}
                >
                  {t.emoji} {t.label}
                </button>
              ))}
            </div>

            {/* Search */}
            <div
              style={{
                padding: "7.5px 9px",
                borderBottom: `1px solid ${C.border}`,
              }}
            >
              <input
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder={`Search ${tab}…`}
                style={{
                  width: "100%",
                  padding: "5.25px 8.25px",
                  background: "var(--bg-input)",
                  border: `1px solid ${C.border}`,
                  borderRadius: 5.25,
                  fontSize: 9,
                  color: C.text,
                  outline: "none",
                  fontFamily: "inherit",
                  boxSizing: "border-box",
                }}
              />
            </div>

            {/* Resource list */}
            <div style={{ flex: 1, overflowY: "auto" }}>
              {loading ? (
                <div
                  style={{
                    display: "flex",
                    justifyContent: "center",
                    padding: 18,
                  }}
                >
                  <Spin size={13.5} />
                </div>
              ) : filtered.length === 0 ? (
                <div
                  style={{
                    padding: 15,
                    textAlign: "center",
                    color: C.muted,
                    fontSize: 9,
                  }}
                >
                  No {tab} found
                </div>
              ) : (
                filtered.map((resource) => (
                  <div
                    key={resource.id}
                    onClick={() => setSelectedResource(resource)}
                    style={{
                      padding: "8.25px 10.5px",
                      borderBottom: `1px solid ${C.border}`,
                      cursor: "pointer",
                      background:
                        selectedResource?.id === resource.id
                          ? "var(--accent-light)"
                          : "transparent",
                      borderLeft: `2.25px solid ${selectedResource?.id === resource.id ? C.accent : "transparent"}`,
                      transition: "all .15s",
                    }}
                    onMouseEnter={(e) => {
                      if (selectedResource?.id !== resource.id)
                        e.currentTarget.style.background =
                          "var(--panel-overlay-minimal)";
                    }}
                    onMouseLeave={(e) => {
                      if (selectedResource?.id !== resource.id)
                        e.currentTarget.style.background = "transparent";
                    }}
                  >
                    <div
                      style={{
                        fontSize: 9.75,
                        fontWeight: 500,
                        color: C.text,
                        whiteSpace: "nowrap",
                        overflow: "hidden",
                        textOverflow: "ellipsis",
                      }}
                    >
                      {resource.title}
                    </div>
                    <div
                      style={{
                        fontSize: 7.5,
                        color: C.disabled,
                        marginTop: 0.75,
                        fontFamily: "monospace",
                      }}
                    >
                      {resource.id}
                    </div>
                  </div>
                ))
              )}
            </div>

            {/* Variant sub-selector (only in Variants tab) */}
            {tab === "variants" && selectedResource && variants.length > 0 && (
              <div
                style={{
                  borderTop: `1px solid ${C.border}`,
                  maxHeight: 165,
                  overflowY: "auto",
                }}
              >
                <div
                  style={{
                    padding: "6px 10.5px 3px",
                    fontSize: 7.5,
                    fontWeight: 700,
                    color: C.muted,
                    textTransform: "uppercase",
                    letterSpacing: ".06em",
                  }}
                >
                  Select variant
                </div>
                {variants.map((v) => (
                  <div
                    key={v.id}
                    onClick={() => setSelectedVariant(v)}
                    style={{
                      padding: "6.75px 10.5px",
                      borderBottom: `1px solid ${C.border}`,
                      cursor: "pointer",
                      background:
                        selectedVariant?.id === v.id
                          ? "var(--accent-light)"
                          : "transparent",
                      borderLeft: `2.25px solid ${selectedVariant?.id === v.id ? C.accent : "transparent"}`,
                      transition: "all .15s",
                    }}
                  >
                    <div
                      style={{ fontSize: 9, fontWeight: 500, color: C.text }}
                    >
                      {v.title}
                    </div>
                    {v.sku && (
                      <div
                        style={{
                          fontSize: 7.5,
                          color: C.disabled,
                          fontFamily: "monospace",
                        }}
                      >
                        SKU: {v.sku}
                      </div>
                    )}
                  </div>
                ))}
              </div>
            )}
          </>
        )}

        {/* Definitions mode - collapsible sections */}
        {mode === "definitions" && (
          <div style={{ flex: 1, overflowY: "auto" }}>
            {loading ? (
              <div
                style={{
                  display: "flex",
                  justifyContent: "center",
                  padding: 18,
                }}
              >
                <Spin size={13.5} />
              </div>
            ) : (
              <>
                {/* Product Definitions Section */}
                <div>
                  <button
                    onClick={() =>
                      setExpandedSections((prev) => ({
                        ...prev,
                        products: !prev.products,
                      }))
                    }
                    style={{
                      width: "100%",
                      padding: "9px 10.5px",
                      background: C.bgCard,
                      border: "none",
                      borderBottom: `1px solid ${C.border}`,
                      fontSize: 9,
                      fontWeight: 600,
                      color: C.text,
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      transition: "all 0.2s",
                      textAlign: "left",
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.background = C.accent;
                      e.currentTarget.style.color = "#fff";
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.background = C.bgCard;
                      e.currentTarget.style.color = C.text;
                    }}
                  >
                    <span style={{ marginLeft: 0 }}>
                      {expandedSections.products ? "▼" : "▶"}
                    </span>
                    📦 Product Metafields ({definitions.products.length})
                  </button>
                  {expandedSections.products &&
                    (definitions.products.length === 0 ? (
                      <div
                        style={{
                          padding: "9px 10.5px",
                          fontSize: 9,
                          color: C.muted,
                        }}
                      >
                        None
                      </div>
                    ) : (
                      definitions.products.map((def) => (
                        <div
                          key={def.id}
                          onClick={() => setSelectedDefinition(def)}
                          style={{
                            padding: "7.5px 10.5px",
                            borderBottom: `1px solid ${C.border}`,
                            cursor: "pointer",
                            background:
                              selectedDefinition?.id === def.id
                                ? "var(--accent-light)"
                                : "transparent",
                            borderLeft: `2.25px solid ${selectedDefinition?.id === def.id ? C.accent : "transparent"}`,
                            transition: "all .15s",
                          }}
                          onMouseEnter={(e) => {
                            if (selectedDefinition?.id !== def.id)
                              e.currentTarget.style.background =
                                "var(--panel-overlay-minimal)";
                          }}
                          onMouseLeave={(e) => {
                            if (selectedDefinition?.id !== def.id)
                              e.currentTarget.style.background = "transparent";
                          }}
                        >
                          <div
                            style={{
                              fontSize: 9,
                              fontWeight: 500,
                              color: C.text,
                              whiteSpace: "nowrap",
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                            }}
                          >
                            {def.name}
                          </div>
                          <div
                            style={{
                              fontSize: 7.5,
                              color: C.disabled,
                              marginTop: 1.5,
                              fontFamily: "monospace",
                            }}
                          >
                            {def.namespace}.{def.key}
                          </div>
                        </div>
                      ))
                    ))}
                </div>

                {/* Collection Definitions Section */}
                <div>
                  <button
                    onClick={() =>
                      setExpandedSections((prev) => ({
                        ...prev,
                        collections: !prev.collections,
                      }))
                    }
                    style={{
                      width: "100%",
                      padding: "9px 10.5px",
                      background: C.bgCard,
                      border: "none",
                      borderBottom: `1px solid ${C.border}`,
                      fontSize: 9,
                      fontWeight: 600,
                      color: C.text,
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      transition: "all 0.2s",
                      textAlign: "left",
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.background = C.accent;
                      e.currentTarget.style.color = "#fff";
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.background = C.bgCard;
                      e.currentInstance.style.color = C.text;
                    }}
                  >
                    <span style={{ marginLeft: 0 }}>
                      {expandedSections.collections ? "▼" : "▶"}
                    </span>
                    📂 Collection Metafields ({definitions.collections.length})
                  </button>
                  {expandedSections.collections &&
                    (definitions.collections.length === 0 ? (
                      <div
                        style={{
                          padding: "9px 10.5px",
                          fontSize: 9,
                          color: C.muted,
                        }}
                      >
                        None
                      </div>
                    ) : (
                      definitions.collections.map((def) => (
                        <div
                          key={def.id}
                          onClick={() => setSelectedDefinition(def)}
                          style={{
                            padding: "7.5px 10.5px",
                            borderBottom: `1px solid ${C.border}`,
                            cursor: "pointer",
                            background:
                              selectedDefinition?.id === def.id
                                ? "var(--accent-light)"
                                : "transparent",
                            borderLeft: `2.25px solid ${selectedDefinition?.id === def.id ? C.accent : "transparent"}`,
                            transition: "all .15s",
                          }}
                          onMouseEnter={(e) => {
                            if (selectedDefinition?.id !== def.id)
                              e.currentTarget.style.background =
                                "var(--panel-overlay-minimal)";
                          }}
                          onMouseLeave={(e) => {
                            if (selectedDefinition?.id !== def.id)
                              e.currentTarget.style.background = "transparent";
                          }}
                        >
                          <div
                            style={{
                              fontSize: 9,
                              fontWeight: 500,
                              color: C.text,
                              whiteSpace: "nowrap",
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                            }}
                          >
                            {def.name}
                          </div>
                          <div
                            style={{
                              fontSize: 7.5,
                              color: C.disabled,
                              marginTop: 1.5,
                              fontFamily: "monospace",
                            }}
                          >
                            {def.namespace}.{def.key}
                          </div>
                        </div>
                      ))
                    ))}
                </div>

                {/* Variant Definitions Section */}
                <div>
                  <button
                    onClick={() =>
                      setExpandedSections((prev) => ({
                        ...prev,
                        variants: !prev.variants,
                      }))
                    }
                    style={{
                      width: "100%",
                      padding: "9px 10.5px",
                      background: C.bgCard,
                      border: "none",
                      borderBottom: `1px solid ${C.border}`,
                      fontSize: 9,
                      fontWeight: 600,
                      color: C.text,
                      cursor: "pointer",
                      display: "flex",
                      alignItems: "center",
                      gap: 6,
                      transition: "all 0.2s",
                      textAlign: "left",
                    }}
                    onMouseEnter={(e) => {
                      e.currentTarget.style.background = C.accent;
                      e.currentTarget.style.color = "#fff";
                    }}
                    onMouseLeave={(e) => {
                      e.currentTarget.style.background = C.bgCard;
                      e.currentTarget.style.color = C.text;
                    }}
                  >
                    <span style={{ marginLeft: 0 }}>
                      {expandedSections.variants ? "▼" : "▶"}
                    </span>
                    🎨 Variant Metafields ({definitions.variants.length})
                  </button>
                  {expandedSections.variants &&
                    (definitions.variants.length === 0 ? (
                      <div
                        style={{
                          padding: "9px 10.5px",
                          fontSize: 9,
                          color: C.muted,
                        }}
                      >
                        None
                      </div>
                    ) : (
                      definitions.variants.map((def) => (
                        <div
                          key={def.id}
                          onClick={() => setSelectedDefinition(def)}
                          style={{
                            padding: "7.5px 10.5px",
                            borderBottom: `1px solid ${C.border}`,
                            cursor: "pointer",
                            background:
                              selectedDefinition?.id === def.id
                                ? "var(--accent-light)"
                                : "transparent",
                            borderLeft: `2.25px solid ${selectedDefinition?.id === def.id ? C.accent : "transparent"}`,
                            transition: "all .15s",
                          }}
                          onMouseEnter={(e) => {
                            if (selectedDefinition?.id !== def.id)
                              e.currentTarget.style.background =
                                "var(--panel-overlay-minimal)";
                          }}
                          onMouseLeave={(e) => {
                            if (selectedDefinition?.id !== def.id)
                              e.currentTarget.style.background = "transparent";
                          }}
                        >
                          <div
                            style={{
                              fontSize: 9,
                              fontWeight: 500,
                              color: C.text,
                              whiteSpace: "nowrap",
                              overflow: "hidden",
                              textOverflow: "ellipsis",
                            }}
                          >
                            {def.name}
                          </div>
                          <div
                            style={{
                              fontSize: 7.5,
                              color: C.disabled,
                              marginTop: 1.5,
                              fontFamily: "monospace",
                            }}
                          >
                            {def.namespace}.{def.key}
                          </div>
                        </div>
                      ))
                    ))}
                </div>
              </>
            )}
          </div>
        )}
      </div>

      {/* ── Right panel: MetafieldsPanel or Definition Details ────────────────────────────────────── */}
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: 21,
          background: "var(--bg-primary)",
        }}
      >
        {mode === "browse" ? (
          <>
            {panelId ? (
              <MetafieldsPanel
                resource={panelResource}
                resourceId={panelId}
                resourceName={panelName}
                toast={toast}
              />
            ) : (
              <div
                style={{
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  justifyContent: "center",
                  height: "100%",
                  color: C.muted,
                  textAlign: "center",
                }}
              >
                <div style={{ fontSize: 42, marginBottom: 12, opacity: 0.4 }}>
                  🏷️
                </div>
                <div
                  style={{
                    fontSize: 12,
                    fontWeight: 600,
                    color: C.text,
                    marginBottom: 4.5,
                  }}
                >
                  {tab === "variants" && selectedResource
                    ? "Now select a variant"
                    : `Select a ${tab === "collections" ? "collection" : tab === "variants" ? "product" : "product"}`}
                </div>
                <div style={{ fontSize: 9.75 }}>
                  to view and manage its metafields
                </div>
              </div>
            )}
          </>
        ) : (
          <>
            {selectedDefinition ? (
              <div>
                <div
                  style={{
                    fontSize: 18,
                    fontWeight: 700,
                    color: C.text,
                    marginBottom: 6,
                    fontFamily: "'Syne', sans-serif",
                  }}
                >
                  {selectedDefinition.name}
                </div>
                <div
                  style={{
                    fontSize: 9.75,
                    color: C.muted,
                    marginBottom: 18,
                  }}
                >
                  Metafield Definition
                </div>

                {/* Definition details card */}
                <div
                  style={{
                    background: C.bgCard,
                    border: `1px solid ${C.border}`,
                    borderRadius: 6,
                    padding: 15,
                  }}
                >
                  <div style={{ marginBottom: 15 }}>
                    <div
                      style={{
                        fontSize: 8.25,
                        fontWeight: 700,
                        color: C.muted,
                        textTransform: "uppercase",
                        letterSpacing: ".06em",
                        marginBottom: 4.5,
                      }}
                    >
                      Name
                    </div>
                    <div style={{ fontSize: 10.5, color: C.text }}>
                      {selectedDefinition.name}
                    </div>
                  </div>

                  <div style={{ marginBottom: 15 }}>
                    <div
                      style={{
                        fontSize: 8.25,
                        fontWeight: 700,
                        color: C.muted,
                        textTransform: "uppercase",
                        letterSpacing: ".06em",
                        marginBottom: 4.5,
                      }}
                    >
                      Namespace
                    </div>
                    <div
                      style={{
                        fontSize: 9.75,
                        color: C.text,
                        fontFamily: "monospace",
                        background: C.bgInput,
                        padding: 6,
                        borderRadius: 3,
                      }}
                    >
                      {selectedDefinition.namespace}
                    </div>
                  </div>

                  <div style={{ marginBottom: 15 }}>
                    <div
                      style={{
                        fontSize: 8.25,
                        fontWeight: 700,
                        color: C.muted,
                        textTransform: "uppercase",
                        letterSpacing: ".06em",
                        marginBottom: 4.5,
                      }}
                    >
                      Key
                    </div>
                    <div
                      style={{
                        fontSize: 9.75,
                        color: C.text,
                        fontFamily: "monospace",
                        background: C.bgInput,
                        padding: 6,
                        borderRadius: 3,
                      }}
                    >
                      {selectedDefinition.key}
                    </div>
                  </div>

                  <div style={{ marginBottom: 15 }}>
                    <div
                      style={{
                        fontSize: 8.25,
                        fontWeight: 700,
                        color: C.muted,
                        textTransform: "uppercase",
                        letterSpacing: ".06em",
                        marginBottom: 4.5,
                      }}
                    >
                      Type
                    </div>
                    <div
                      style={{
                        fontSize: 9.75,
                        display: "inline-block",
                        background: C.accent,
                        color: "#fff",
                        padding: "4.5px 9px",
                        borderRadius: 3,
                        fontWeight: 500,
                      }}
                    >
                      {selectedDefinition.type?.name ||
                        selectedDefinition.type ||
                        "Unknown"}
                    </div>
                  </div>

                  {selectedDefinition.description && (
                    <div style={{ marginBottom: 15 }}>
                      <div
                        style={{
                          fontSize: 8.25,
                          fontWeight: 700,
                          color: C.muted,
                          textTransform: "uppercase",
                          letterSpacing: ".06em",
                          marginBottom: 4.5,
                        }}
                      >
                        Description
                      </div>
                      <div
                        style={{
                          fontSize: 9.75,
                          color: C.text,
                          lineHeight: 1.5,
                        }}
                      >
                        {selectedDefinition.description}
                      </div>
                    </div>
                  )}

                  {selectedDefinition.validations &&
                    selectedDefinition.validations.length > 0 && (
                      <div>
                        <div
                          style={{
                            fontSize: 8.25,
                            fontWeight: 700,
                            color: C.muted,
                            textTransform: "uppercase",
                            letterSpacing: ".06em",
                            marginBottom: 4.5,
                          }}
                        >
                          Validations
                        </div>
                        <div
                          style={{
                            fontSize: 9.75,
                            color: C.text,
                            background: C.bgInput,
                            padding: 9,
                            borderRadius: 3,
                            fontFamily: "monospace",
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-word",
                          }}
                        >
                          {JSON.stringify(
                            selectedDefinition.validations,
                            null,
                            2,
                          )}
                        </div>
                      </div>
                    )}
                </div>
              </div>
            ) : (
              <div
                style={{
                  display: "flex",
                  flexDirection: "column",
                  alignItems: "center",
                  justifyContent: "center",
                  height: "100%",
                  color: C.muted,
                  textAlign: "center",
                }}
              >
                <div style={{ fontSize: 42, marginBottom: 12, opacity: 0.4 }}>
                  ⚙️
                </div>
                <div
                  style={{
                    fontSize: 12,
                    fontWeight: 600,
                    color: C.text,
                    marginBottom: 4.5,
                  }}
                >
                  No metafield selected
                </div>
                <div style={{ fontSize: 9.75 }}>
                  Click on a metafield definition to view and edit its details
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </div>
  );
}
