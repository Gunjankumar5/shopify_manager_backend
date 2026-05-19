import { useState } from "react";
import { Ico, Spin } from "./Icons";
import { Modal, Field } from "./UI";

export const PriceModal = ({ count, onApply, onClose }) => {
  const [mode, setMode] = useState("percent");
  const [val, setVal] = useState("");
  const [dir, setDir] = useState("increase");
  const preview = val
    ? mode === "percent"
      ? dir === "increase"
        ? (100 * (1 + parseFloat(val) / 100)).toFixed(2)
        : (100 * (1 - parseFloat(val) / 100)).toFixed(2)
      : dir === "increase"
        ? (100 + parseFloat(val)).toFixed(2)
        : (100 - parseFloat(val)).toFixed(2)
    : null;
  return (
    <Modal
      title="Adjust Prices"
      subtitle={`${count} product${count !== 1 ? "s" : ""} selected`}
      onClose={onClose}
      maxWidth={500}
    >
      <div style={{ display: "flex", flexDirection: "column", gap: 15 }}>
        <div>
          <p
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: "var(--text-secondary)",
              marginBottom: 8,
              textTransform: "uppercase",
            }}
          >
            Type
          </p>
          <div style={{ display: "flex", gap: 8 }}>
            {[
              { id: "percent", label: "% Percentage" },
              { id: "fixed", label: "$ Fixed" },
            ].map((m) => (
              <button
                key={m.id}
                onClick={() => setMode(m.id)}
                className={`btn ${mode === m.id ? "btn-primary" : "btn-secondary"}`}
                style={{ flex: 1 }}
              >
                {m.label}
              </button>
            ))}
          </div>
        </div>
        <div>
          <p
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: "var(--text-secondary)",
              marginBottom: 8,
              textTransform: "uppercase",
            }}
          >
            Direction
          </p>
          <div style={{ display: "flex", gap: 8 }}>
            {[
              { id: "increase", label: "↑ Increase", color: "var(--success)" },
              { id: "decrease", label: "↓ Decrease", color: "var(--danger)" },
            ].map((d) => (
              <button
                key={d.id}
                onClick={() => setDir(d.id)}
                className="btn btn-secondary"
                style={{
                  flex: 1,
                  borderColor: dir === d.id ? d.color : undefined,
                  background: dir === d.id ? `${d.color}15` : undefined,
                  color: dir === d.id ? d.color : undefined,
                }}
              >
                {d.label}
              </button>
            ))}
          </div>
        </div>
        <Field
          label={mode === "percent" ? "Percentage" : "Amount"}
          value={val}
          onChange={setVal}
          type="number"
          suf={mode === "percent" ? "%" : "$"}
        />
        {preview && (
          <div
            style={{
              background: "rgba(99,102,241,0.08)",
              border: "1px solid rgba(99,102,241,0.2)",
              borderRadius: 9,
              padding: "10.5px 12px",
              fontSize: 10.5,
              color: "var(--accent)",
            }}
          >
            $100.00 →{" "}
            <strong
              style={{
                color: dir === "increase" ? "var(--success)" : "var(--danger)",
              }}
            >
              ${preview}
            </strong>
          </div>
        )}
        <div style={{ display: "flex", gap: 9, paddingTop: 6 }}>
          <button
            onClick={() =>
              val && onApply({ mode, value: parseFloat(val), dir })
            }
            disabled={!val}
            className="btn btn-primary"
            style={{ flex: 1, padding: "12px" }}
          >
            Apply to {count} Products
          </button>
          <button
            onClick={onClose}
            className="btn btn-secondary"
            style={{ padding: "12px 24px" }}
          >
            Cancel
          </button>
        </div>
      </div>
    </Modal>
  );
};

export const EditModal = ({ product, onSave, onClose }) => {
  const [f, setF] = useState({
    title: product?.title || "",
    vendor: product?.vendor || "",
    product_type: product?.product_type || "",
    tags: product?.tags || "",
    status: product?.status || "draft",
    body_html: product?.body_html || "",
    price: product?.variants?.[0]?.price || "",
    compare_at_price: product?.variants?.[0]?.compare_at_price || "",
    sku: product?.variants?.[0]?.sku || "",
    inventory_quantity: product?.variants?.[0]?.inventory_quantity || "",
    image_src: product?.images?.[0]?.src || "",
  });
  const [saving, setSaving] = useState(false);
  const update = (key) => (val) => setF((prev) => ({ ...prev, [key]: val }));
  const save = async () => {
    setSaving(true);
    await onSave({
      title: f.title,
      vendor: f.vendor,
      product_type: f.product_type,
      tags: f.tags,
      status: f.status,
      body_html: f.body_html,
      variants: [
        {
          price: f.price,
          compare_at_price: f.compare_at_price,
          sku: f.sku,
          inventory_quantity: parseInt(f.inventory_quantity) || 0,
        },
      ],
      ...(f.image_src ? { images: [{ src: f.image_src }] } : {}),
    });
    setSaving(false);
  };
  return (
    <Modal
      title={product ? "Edit Product" : "Add Product"}
      subtitle={product?.title}
      onClose={onClose}
      maxWidth={740}
    >
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 12 }}>
        <div style={{ gridColumn: "1/-1" }}>
          <Field label="Title *" value={f.title} onChange={update("title")} />
        </div>
        <Field label="Vendor" value={f.vendor} onChange={update("vendor")} />
        <Field
          label="Product Type"
          value={f.product_type}
          onChange={update("product_type")}
        />
        <Field label="Tags" value={f.tags} onChange={update("tags")} />
        <Field
          label="Status"
          value={f.status}
          onChange={update("status")}
          options={[
            { v: "draft", l: "Draft" },
            { v: "active", l: "Active" },
            { v: "archived", l: "Archived" },
          ]}
        />
        <div
          style={{
            gridColumn: "1/-1",
            borderTop: "1px solid var(--border-color)",
            paddingTop: 16,
            marginTop: 8,
          }}
        >
          <p
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: "var(--text-muted)",
              textTransform: "uppercase",
              marginBottom: 12,
            }}
          >
            Pricing & Inventory
          </p>
        </div>
        <Field
          label="Price"
          value={f.price}
          onChange={update("price")}
          type="number"
          pre="$"
        />
        <Field
          label="Compare At Price"
          value={f.compare_at_price}
          onChange={update("compare_at_price")}
          type="number"
          pre="$"
        />
        <Field label="SKU" value={f.sku} onChange={update("sku")} />
        <Field
          label="Inventory Qty"
          value={f.inventory_quantity}
          onChange={update("inventory_quantity")}
          type="number"
        />
        <div
          style={{
            gridColumn: "1/-1",
            borderTop: "1px solid var(--border-color)",
            paddingTop: 16,
            marginTop: 8,
          }}
        >
          <p
            style={{
              fontSize: 12,
              fontWeight: 600,
              color: "var(--text-muted)",
              textTransform: "uppercase",
              marginBottom: 12,
            }}
          >
            Media & Description
          </p>
        </div>
        <div style={{ gridColumn: "1/-1" }}>
          <Field
            label="Image URL"
            value={f.image_src}
            onChange={update("image_src")}
          />
        </div>
        <div style={{ gridColumn: "1/-1" }}>
          <Field
            label="Description (HTML)"
            value={f.body_html}
            onChange={update("body_html")}
            type="textarea"
            rows={4}
          />
        </div>
        <div
          style={{
            gridColumn: "1/-1",
            display: "flex",
            gap: 12,
            paddingTop: 8,
          }}
        >
          <button
            onClick={save}
            disabled={saving || !f.title}
            className="btn btn-primary"
            style={{ flex: 1, padding: "13px" }}
          >
            {saving ? (
              <span
                style={{
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  gap: 8,
                }}
              >
                <Spin size={16} /> Saving...
              </span>
            ) : (
              "Save to Shopify"
            )}
          </button>
          <button
            onClick={onClose}
            className="btn btn-secondary"
            style={{ padding: "13px 24px" }}
          >
            Cancel
          </button>
        </div>
      </div>
    </Modal>
  );
};
