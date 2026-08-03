import { Ico } from "./Icons";
import { Badge } from "./UI";
import { memo, useMemo } from "react";

const ProductCard = memo(({ p, sel, onSel, onEdit, onDel }) => {
  const img = p.images?.[0]?.src || p.image?.src;
  const price = p.variants?.[0]?.price;
  const cmp = p.variants?.[0]?.compare_at_price;

  const { inv, invStatus } = useMemo(() => {
    const inventory =
      p.variants?.reduce((s, v) => s + (v.inventory_quantity || 0), 0) || 0;

    let status;
    if (inventory === 0) {
      status = { color: "var(--danger)",  bg: "rgba(239,68,68,0.1)",   label: "Out of stock"        };
    } else if (inventory < 5) {
      status = { color: "var(--warning)", bg: "rgba(245,158,11,0.1)",  label: `Only ${inventory} left` };
    } else {
      status = { color: "var(--success)", bg: "rgba(16,185,129,0.1)",  label: `${inventory} in stock`  };
    }
    return { inv: inventory, invStatus: status };
  }, [p.variants]);

  // Build metafield display list — use display name if available
  const metafieldItems = useMemo(() => {
    if (!p.metafields || !p.metafields.length) return [];
    return p.metafields
      .filter(mf => mf.value !== null && mf.value !== undefined && mf.value !== "")
      .slice(0, 3)
      .map(mf => ({
        label: mf.key.replace(/_/g, " "),
        value: typeof mf.value === "object"
          ? JSON.stringify(mf.value)
          : String(mf.value),
        ns: mf.namespace,
      }));
  }, [p.metafields]);

  return (
    <div
      className="card-hover"
      style={{
        background: "var(--bg-card)",
        border: `2px solid ${sel ? "var(--accent)" : "var(--border-strong)"}`,
        borderRadius: "var(--radius-xl)",
        overflow: "hidden",
        position: "relative",
        transition: "border-color 0.2s ease, box-shadow 0.2s ease",
        boxShadow: "var(--shadow-sm)",
      }}
    >
      {/* Selection checkbox */}
      <div style={{
        position: "absolute", top: "var(--space-3)", left: "var(--space-3)",
        zIndex: 5, background: "rgba(0,0,0,0.3)", borderRadius: "var(--radius-sm)",
        padding: "2px", backdropFilter: "blur(2px)",
      }}>
        <input
          type="checkbox" className="chk" checked={sel}
          onChange={() => onSel(p.id)} aria-label={`Select ${p.title}`}
        />
      </div>

      {/* Sale badge */}
      {cmp && (
        <div style={{
          position: "absolute", top: "var(--space-3)",
          left: "calc(var(--space-8) + 8px)", zIndex: 5,
          background: "var(--accent)", color: "white",
          fontSize: "var(--text-xs)", fontWeight: 700,
          padding: "2px 8px", borderRadius: "var(--radius-full)",
          boxShadow: "var(--shadow-sm)", textTransform: "uppercase", letterSpacing: "0.5px",
        }}>
          Sale
        </div>
      )}

      {/* Status badge */}
      <div style={{ position: "absolute", top: "var(--space-3)", right: "var(--space-3)", zIndex: 5 }}>
        <Badge status={p.status} />
      </div>

      {/* Image */}
      <div style={{
        height: 170, background: "var(--bg-secondary)",
        display: "flex", alignItems: "center", justifyContent: "center",
        overflow: "hidden", position: "relative",
      }}>
        {img ? (
          <>
            <img
              src={img} alt={p.title}
              style={{ width: "100%", height: "100%", objectFit: "cover", transition: "transform 0.3s var(--transition-base)", willChange: "transform" }}
              onMouseEnter={(e) => (e.currentTarget.style.transform = "scale(1.08)")}
              onMouseLeave={(e) => (e.currentTarget.style.transform = "scale(1)")}
            />
            <div
              style={{
                position: "absolute", inset: 0, background: "rgba(0,0,0,0.3)", opacity: 0,
                transition: "opacity 0.2s ease", display: "flex", alignItems: "center",
                justifyContent: "center", pointerEvents: "none",
              }}
              className="image-overlay"
            >
              <Ico n="eye" size={24} color="white" />
            </div>
            <style>{`.card-hover:hover .image-overlay { opacity: 1; }`}</style>
          </>
        ) : (
          <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: "var(--space-1)", color: "var(--border-strong)" }}>
            <Ico n="image" size={36} color="var(--border-strong)" />
            <span style={{ fontSize: "var(--text-xs)" }}>No image</span>
          </div>
        )}
      </div>

      {/* Content */}
      <div style={{ padding: "var(--space-3)" }}>
        {/* Title and vendor */}
        <div style={{ marginBottom: "var(--space-2)" }}>
          <h3 style={{
            fontFamily: "var(--font-display)", fontSize: "var(--text-base)", fontWeight: 700,
            color: "var(--text-primary)", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
          }}>
            {p.title}
          </h3>
          {p.vendor && (
            <div style={{ display: "flex", alignItems: "center", gap: "4px", fontSize: "var(--text-xs)", color: "var(--text-muted)", marginTop: "2px" }}>
              <Ico n="store" size={12} color="var(--text-muted)" />
              <span>{p.vendor}</span>
            </div>
          )}
        </div>

        {/* Price & stock */}
        <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "var(--space-3)" }}>
          <div style={{ display: "flex", alignItems: "baseline", gap: "var(--space-2)", flexWrap: "wrap" }}>
            <span style={{ fontSize: "var(--text-lg)", fontWeight: 700, color: "var(--accent)" }}>
              {price ? `$${price}` : "—"}
            </span>
            {cmp && (
              <span style={{ fontSize: "var(--text-xs)", color: "var(--text-muted)", textDecoration: "line-through" }}>
                ${cmp}
              </span>
            )}
          </div>
          <span style={{
            fontSize: "var(--text-xs)", color: invStatus.color, background: invStatus.bg,
            padding: "var(--space-1) var(--space-2)", borderRadius: "var(--radius-full)",
            fontWeight: 600, whiteSpace: "nowrap",
          }}>
            {invStatus.label}
          </span>
        </div>

        {/* Metafields — show up to 3 */}
        {metafieldItems.length > 0 && (
          <div style={{
            marginBottom: "var(--space-3)",
            padding: "var(--space-2) var(--space-3)",
            background: "var(--bg-secondary)",
            borderRadius: "var(--radius-md)",
            border: "1px solid var(--border-strong)",
          }}>
            <div style={{
              fontSize: 9, fontWeight: 700, color: "var(--text-muted)",
              textTransform: "uppercase", letterSpacing: "0.6px", marginBottom: 6,
            }}>
              Metafields
            </div>
            {metafieldItems.map((mf, i) => (
              <div key={i} style={{
                display: "flex", justifyContent: "space-between",
                alignItems: "center", fontSize: 11, gap: 8,
                marginBottom: i < metafieldItems.length - 1 ? 4 : 0,
              }}>
                <span style={{
                  color: "var(--text-muted)", overflow: "hidden",
                  textOverflow: "ellipsis", whiteSpace: "nowrap", maxWidth: "45%",
                  textTransform: "capitalize",
                }}>
                  {mf.label}
                </span>
                <span style={{
                  color: "var(--text-primary)", overflow: "hidden",
                  textOverflow: "ellipsis", whiteSpace: "nowrap",
                  maxWidth: "55%", textAlign: "right",
                }}>
                  {mf.value.length > 20 ? mf.value.slice(0, 20) + "…" : mf.value}
                </span>
              </div>
            ))}
            {(p.metafields?.length || 0) > 3 && (
              <div style={{ fontSize: 10, color: "var(--accent)", marginTop: 4 }}>
                +{p.metafields.length - 3} more
              </div>
            )}
          </div>
        )}

        {/* Action buttons */}
        <div style={{ display: "flex", gap: "var(--space-2)" }}>
          <button
            onClick={() => onEdit(p)} className="btn btn-secondary"
            style={{ flex: 1, padding: "var(--space-2) var(--space-3)", fontSize: "var(--text-xs)" }}
            aria-label="Edit product" title="Edit product"
          >
            <Ico n="edit" size={13} color="var(--accent)" /> Edit
          </button>
          <button
            onClick={() => onDel(p.id)} className="btn btn-danger"
            style={{ padding: "var(--space-2) var(--space-3)" }}
            aria-label="Delete product" title="Delete product"
          >
            <Ico n="trash" size={13} color="var(--danger)" />
          </button>
        </div>
      </div>
    </div>
  );
});

ProductCard.displayName = "ProductCard";
export default ProductCard;
