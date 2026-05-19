import { useState, useRef, useEffect, useMemo } from "react";
import { api } from "../api/api";
import { Spin } from "../components/Icons";
import { PageLoadingOverlay } from "../components/UI";
import MetafieldsPanel from "../components/MetafieldsPanel";

function useViewportWidth() {
  const [width, setWidth] = useState(() => window.innerWidth);

  useEffect(() => {
    const onResize = () => setWidth(window.innerWidth);
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  return width;
}

// ── Design tokens using CSS variables for theme support ────────────────
const C = {
  card: "var(--bg-card)",
  border: "var(--border-subtle)",
  borderFocus: "var(--accent)",
  text: "var(--text-primary)",
  muted: "var(--text-secondary)",
  disabled: "var(--text-muted)",
  accent: "var(--accent)",
  success: "var(--success)",
  danger: "var(--danger)",
  warning: "var(--warning)",
  bgInput: "var(--bg-input)",
};

// ── Primitives ────────────────────────────────────────────────────────────
const Card = ({ children, style }) => (
  <div
    style={{
      background: C.card,
      border: `1px solid ${C.border}`,
      borderRadius: 10,
      marginBottom: 16,
      overflow: "hidden",
      ...style,
    }}
  >
    {children}
  </div>
);

const CardTitle = ({ children, right }) => (
  <div
    style={{
      padding: "14px 18px",
      borderBottom: `1px solid ${C.border}`,
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
      fontSize: 13,
      fontWeight: 600,
      color: C.text,
    }}
  >
    <span>{children}</span>
    {right}
  </div>
);

const CardBody = ({ children, style }) => (
  <div style={{ padding: "16px 18px", ...style }}>{children}</div>
);

const Field = ({ label, optional, children, help, style: s }) => (
  <div style={{ marginBottom: 14, ...s }}>
    {label && (
      <label
        style={{
          display: "block",
          fontSize: 12,
          fontWeight: 500,
          color: C.muted,
          marginBottom: 5,
        }}
      >
        {label}
        {optional && (
          <span style={{ fontWeight: 400, color: C.disabled, marginLeft: 4 }}>
            (optional)
          </span>
        )}
      </label>
    )}
    {children}
    {help && (
      <div style={{ fontSize: 11, color: C.disabled, marginTop: 3 }}>
        {help}
      </div>
    )}
  </div>
);

function useInputStyle(focused) {
  return {
    width: "100%",
    padding: "8px 12px",
    background: C.bgInput,
    border: `1px solid ${focused ? C.borderFocus : C.border}`,
    borderRadius: 7,
    fontSize: 13,
    color: C.text,
    outline: "none",
    fontFamily: "inherit",
    boxShadow: focused ? `0 0 0 3px var(--accent-light)` : "none",
    transition: "border-color .15s, box-shadow .15s",
  };
}

function Input({
  value,
  onChange,
  placeholder,
  type = "text",
  prefix,
  readOnly,
  style: s,
}) {
  const [focused, setFocused] = useState(false);
  const base = useInputStyle(focused);
  if (prefix)
    return (
      <div
        style={{
          display: "flex",
          alignItems: "center",
          borderRadius: 7,
          overflow: "hidden",
          border: `1px solid ${focused ? C.borderFocus : C.border}`,
          boxShadow: focused ? `0 0 0 3px rgba(59,130,246,.12)` : "none",
          transition: "border-color .15s, box-shadow .15s",
        }}
      >
        <span
          style={{
            padding: "8px 10px",
            background: C.bgInput,
            borderRight: `1px solid ${C.border}`,
            fontSize: 12,
            color: C.muted,
            whiteSpace: "nowrap",
          }}
        >
          {prefix}
        </span>
        <input
          type={type}
          value={value}
          onChange={onChange}
          placeholder={placeholder}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          style={{
            ...base,
            border: "none",
            borderRadius: 0,
            boxShadow: "none",
            flex: 1,
            ...s,
          }}
        />
      </div>
    );
  return (
    <input
      type={type}
      value={value}
      onChange={onChange}
      placeholder={placeholder}
      readOnly={readOnly}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      style={{
        ...base,
        ...(readOnly
          ? { background: C.bgInput, color: C.disabled, cursor: "default" }
          : {}),
        ...s,
      }}
    />
  );
}

function Textarea({ value, onChange, placeholder, minHeight = 80 }) {
  const [focused, setFocused] = useState(false);
  return (
    <textarea
      value={value}
      onChange={onChange}
      placeholder={placeholder}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
      style={{
        ...useInputStyle(focused),
        resize: "vertical",
        minHeight,
        fontFamily: "inherit",
      }}
    />
  );
}

function Select({ value, onChange, options }) {
  const [focused, setFocused] = useState(false);
  return (
    <div style={{ position: "relative" }}>
      <select
        value={value}
        onChange={onChange}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        style={{
          ...useInputStyle(focused),
          appearance: "none",
          paddingRight: 32,
          cursor: "pointer",
        }}
      >
        {options.map((o) => (
          <option key={o.v} value={o.v}>
            {o.l}
          </option>
        ))}
      </select>
      <svg
        style={{
          position: "absolute",
          right: 10,
          top: "50%",
          transform: "translateY(-50%)",
          pointerEvents: "none",
        }}
        width="12"
        height="12"
        viewBox="0 0 24 24"
        fill="none"
        stroke={C.muted}
        strokeWidth="2"
      >
        <path d="M6 9l6 6 6-6" />
      </svg>
    </div>
  );
}

function Btn({ children, onClick, disabled, variant = "secondary", style: s }) {
  const [hov, setHov] = useState(false);
  const variants = {
    primary: {
      background: hov ? "#2563eb" : C.accent,
      color: "#fff",
      border: "none",
    },
    secondary: {
      background: hov ? "#222" : C.card,
      color: C.muted,
      border: `1px solid ${C.border}`,
    },
    danger: {
      background: hov ? "rgba(239,68,68,.15)" : "transparent",
      color: C.danger,
      border: `1px solid ${hov ? C.danger : C.border}`,
    },
    ghost: {
      background: "transparent",
      color: C.muted,
      border: "none",
      padding: "4px 10px",
    },
  };
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      onMouseEnter={() => setHov(true)}
      onMouseLeave={() => setHov(false)}
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 6,
        padding: "8px 16px",
        borderRadius: 7,
        fontSize: 13,
        fontWeight: 600,
        fontFamily: "inherit",
        cursor: disabled ? "not-allowed" : "pointer",
        transition: "all .15s",
        opacity: disabled ? 0.5 : 1,
        ...variants[variant],
        ...s,
      }}
    >
      {children}
    </button>
  );
}

function CollectionTypeSwitch({ value, onChange, disabled }) {
  const isSmart = value === "smart";

  return (
    <div>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          gap: 12,
          marginBottom: 10,
        }}
      >
        <div>
          <div style={{ fontSize: 13, fontWeight: 600, color: C.text }}>
            {isSmart ? "Smart collection" : "Manual collection"}
          </div>
          <div style={{ fontSize: 11, color: C.muted, marginTop: 3 }}>
            {isSmart
              ? "Products are included automatically from the rules below."
              : "Products are managed manually in Shopify."}
          </div>
        </div>
        <button
          type="button"
          onClick={() => onChange(isSmart ? "custom" : "smart")}
          disabled={disabled}
          aria-pressed={isSmart}
          aria-label="Switch collection type"
          style={{
            width: 52,
            height: 30,
            borderRadius: 999,
            border: `1px solid ${isSmart ? C.accent : C.border}`,
            background: isSmart ? C.accent : "#0d0d0d",
            padding: 3,
            position: "relative",
            cursor: disabled ? "not-allowed" : "pointer",
            opacity: disabled ? 0.6 : 1,
            transition: "all .18s",
          }}
        >
          <span
            style={{
              display: "block",
              width: 22,
              height: 22,
              borderRadius: "50%",
              background: "var(--bg-primary)",
              transform: `translateX(${isSmart ? 22 : 0}px)`,
              transition: "transform .18s",
            }}
          />
        </button>
      </div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 8,
        }}
      >
        {[
          {
            id: "custom",
            title: "Manual",
            note: "Manually assign products",
          },
          {
            id: "smart",
            title: "Smart",
            note: "Auto-build from rules",
          },
        ].map((option) => {
          const active = value === option.id;
          return (
            <button
              key={option.id}
              type="button"
              disabled={disabled}
              onClick={() => onChange(option.id)}
              style={{
                textAlign: "left",
                border: `1px solid ${active ? C.accent : C.border}`,
                background: active ? "var(--accent-light)" : C.bgInput,
                borderRadius: 8,
                padding: "10px 12px",
                cursor: disabled ? "not-allowed" : "pointer",
                transition: "all .15s",
              }}
            >
              <div style={{ fontSize: 12, fontWeight: 600, color: C.text }}>
                {option.title}
              </div>
              <div style={{ fontSize: 11, color: C.muted, marginTop: 3 }}>
                {option.note}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}

// ── Rich text ─────────────────────────────────────────────────────────────
function RichText({ valueRef, initialHtml }) {
  const [focused, setFocused] = useState(false);
  useEffect(() => {
    if (valueRef.current && initialHtml) {
      valueRef.current.innerHTML = initialHtml;
    }
  }, [initialHtml]);

  const tools = [
    { cmd: "bold", label: <b>B</b> },
    { cmd: "italic", label: <i>I</i> },
    { cmd: "underline", label: <u>U</u> },
    null,
    { cmd: "insertUnorderedList", label: "≡" },
    { cmd: "insertOrderedList", label: "⊟" },
    null,
    { cmd: "justifyLeft", label: "⬅" },
    { cmd: "justifyCenter", label: "↔" },
  ];

  return (
    <div
      style={{
        border: `1px solid ${focused ? C.borderFocus : C.border}`,
        borderRadius: 7,
        overflow: "hidden",
        boxShadow: focused ? `0 0 0 3px rgba(59,130,246,.12)` : "none",
        transition: "border-color .15s, box-shadow .15s",
      }}
    >
      <div
        style={{
          display: "flex",
          gap: 2,
          padding: 8,
          background: C.bgInput,
          borderBottom: `1px solid ${C.border}`,
          flexWrap: "wrap",
        }}
      >
        {tools.map((t, i) =>
          t === null ? (
            <div
              key={i}
              style={{ width: 1, background: C.border, margin: "2px 2px" }}
            />
          ) : (
            <button
              key={t.cmd}
              type="button"
              onClick={() => {
                document.execCommand(t.cmd, false, null);
                valueRef.current?.focus();
              }}
              style={{
                width: 28,
                height: 28,
                border: "none",
                background: "transparent",
                borderRadius: 4,
                cursor: "pointer",
                fontSize: 13,
                color: C.muted,
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                fontFamily: "inherit",
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background = C.border;
                e.currentTarget.style.color = C.text;
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = "transparent";
                e.currentTarget.style.color = C.muted;
              }}
            >
              {t.label}
            </button>
          ),
        )}
      </div>
      <div
        ref={valueRef}
        contentEditable
        suppressContentEditableWarning
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        style={{
          padding: 12,
          minHeight: 140,
          outline: "none",
          fontSize: 13,
          color: C.text,
          background: C.bgInput,
          lineHeight: 1.6,
        }}
        data-placeholder="Describe this collection..."
      />
    </div>
  );
}

// ── Image upload card ──────────────────────────────────────────────────────
function CollectionImageCard({ imageUrl, imageFile, onFileChange, onRemove }) {
  const [drag, setDrag] = useState(false);
  const inputRef = useRef(null);

  function handleFiles(files) {
    const file = files[0];
    if (!file || !file.type.startsWith("image/")) return;
    onFileChange(file);
  }

  const previewSrc = imageFile
    ? URL.createObjectURL(imageFile)
    : imageUrl || null;

  return (
    <Card>
      <CardTitle>Collection image</CardTitle>
      <CardBody>
        {previewSrc ? (
          <>
            <img
              src={previewSrc}
              alt="Collection"
              style={{
                width: "100%",
                aspectRatio: "1",
                objectFit: "cover",
                borderRadius: 7,
                border: `1px solid ${C.border}`,
                display: "block",
              }}
            />
            <div
              style={{
                display: "flex",
                gap: 8,
                marginTop: 10,
                flexWrap: "wrap",
              }}
            >
              <Btn
                onClick={() => inputRef.current?.click()}
                variant="secondary"
                style={{ fontSize: 12, padding: "5px 12px" }}
              >
                Replace
              </Btn>
              <Btn
                onClick={onRemove}
                variant="danger"
                style={{ fontSize: 12, padding: "5px 12px" }}
              >
                Remove
              </Btn>
            </div>
            <input
              ref={inputRef}
              type="file"
              accept="image/*"
              style={{ display: "none" }}
              onChange={(e) => handleFiles(e.target.files)}
            />
          </>
        ) : (
          <div
            onDragOver={(e) => {
              e.preventDefault();
              setDrag(true);
            }}
            onDragLeave={() => setDrag(false)}
            onDrop={(e) => {
              e.preventDefault();
              setDrag(false);
              handleFiles(e.dataTransfer.files);
            }}
            onClick={() => inputRef.current?.click()}
            style={{
              border: `2px dashed ${drag ? C.accent : C.border}`,
              borderRadius: 8,
              padding: "32px 20px",
              textAlign: "center",
              cursor: "pointer",
              background: drag ? "rgba(59,130,246,.05)" : "transparent",
              transition: "all .2s",
            }}
          >
            <input
              ref={inputRef}
              type="file"
              accept="image/*"
              style={{ display: "none" }}
              onChange={(e) => handleFiles(e.target.files)}
            />
            <div style={{ fontSize: 24, marginBottom: 8 }}>🖼</div>
            <div
              style={{
                fontSize: 13,
                fontWeight: 600,
                color: C.text,
                marginBottom: 3,
              }}
            >
              Add image
            </div>
            <div style={{ fontSize: 12, color: C.muted }}>
              Upload or drag and drop
            </div>
          </div>
        )}
      </CardBody>
    </Card>
  );
}

// ── Status dot ─────────────────────────────────────────────────────────────
function StatusDot({ status }) {
  const map = { active: C.success, draft: C.muted, archived: C.danger };
  return (
    <div
      style={{
        display: "inline-flex",
        alignItems: "center",
        gap: 7,
        fontSize: 13,
        fontWeight: 500,
        color: map[status] || C.muted,
      }}
    >
      <span
        style={{
          width: 8,
          height: 8,
          borderRadius: "50%",
          background: map[status] || C.muted,
          display: "inline-block",
        }}
      />
      {status.charAt(0).toUpperCase() + status.slice(1)}
    </div>
  );
}

// ── Smart conditions ───────────────────────────────────────────────────────
const COND_FIELDS = [
  { value: "title", label: "Product title" },
  { value: "type", label: "Product type" },
  { value: "vendor", label: "Product vendor" },
  { value: "variant_price", label: "Price" },
  { value: "tag", label: "Product tag" },
  { value: "variant_weight", label: "Weight" },
  { value: "variant_inventory", label: "Inventory stock" },
];
const NUMERIC_FIELDS = new Set([
  "variant_price",
  "variant_weight",
  "variant_inventory",
]);
const TEXT_OPS = [
  { v: "equals", l: "is equal to" },
  { v: "not_equals", l: "is not equal to" },
  { v: "starts_with", l: "starts with" },
  { v: "ends_with", l: "ends with" },
  { v: "contains", l: "contains" },
  { v: "not_contains", l: "does not contain" },
];
const NUM_OPS = [
  { v: "greater_than", l: "is greater than" },
  { v: "less_than", l: "is less than" },
  { v: "equals", l: "is equal to" },
  { v: "not_equals", l: "is not equal to" },
];

let _condId = 0;
const newCond = () => ({
  _id: ++_condId,
  field: "title",
  operator: "equals",
  value: "",
});

function ConditionRow({ cond, onChange, onRemove, isOnly, compact }) {
  const isNum = NUMERIC_FIELDS.has(cond.field);
  const ops = isNum ? NUM_OPS : TEXT_OPS;
  const [focused, setFocused] = useState(false);

  return (
    <div
      style={{
        display: "grid",
        gridTemplateColumns: compact ? "1fr" : "1fr 1fr 1fr 36px",
        gap: 8,
        alignItems: "center",
        marginBottom: 10,
      }}
    >
      {/* Field */}
      <div style={{ position: "relative" }}>
        <select
          value={cond.field}
          onChange={(e) => onChange("field", e.target.value)}
          style={{
            ...useInputStyle(false),
            appearance: "none",
            paddingRight: 28,
            cursor: "pointer",
          }}
        >
          {COND_FIELDS.map((f) => (
            <option key={f.value} value={f.value}>
              {f.label}
            </option>
          ))}
        </select>
        <svg
          style={{
            position: "absolute",
            right: 8,
            top: "50%",
            transform: "translateY(-50%)",
            pointerEvents: "none",
          }}
          width="10"
          height="10"
          viewBox="0 0 24 24"
          fill="none"
          stroke={C.muted}
          strokeWidth="2"
        >
          <path d="M6 9l6 6 6-6" />
        </svg>
      </div>

      {/* Operator */}
      <div style={{ position: "relative" }}>
        <select
          value={cond.operator}
          onChange={(e) => onChange("operator", e.target.value)}
          style={{
            ...useInputStyle(false),
            appearance: "none",
            paddingRight: 28,
            cursor: "pointer",
          }}
        >
          {ops.map((o) => (
            <option key={o.v} value={o.v}>
              {o.l}
            </option>
          ))}
        </select>
        <svg
          style={{
            position: "absolute",
            right: 8,
            top: "50%",
            transform: "translateY(-50%)",
            pointerEvents: "none",
          }}
          width="10"
          height="10"
          viewBox="0 0 24 24"
          fill="none"
          stroke={C.muted}
          strokeWidth="2"
        >
          <path d="M6 9l6 6 6-6" />
        </svg>
      </div>

      {/* Value */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          borderRadius: 7,
          overflow: "hidden",
          border: `1px solid ${focused ? C.borderFocus : C.border}`,
          boxShadow: focused ? `0 0 0 3px rgba(59,130,246,.12)` : "none",
          transition: "border-color .15s, box-shadow .15s",
        }}
      >
        {cond.field === "variant_price" && (
          <span
            style={{
              padding: "8px 8px",
              background: C.bgInput,
              borderRight: `1px solid ${C.border}`,
              fontSize: 12,
              color: C.muted,
            }}
          >
            $
          </span>
        )}
        <input
          type={isNum ? "number" : "text"}
          value={cond.value}
          onChange={(e) => onChange("value", e.target.value)}
          placeholder={isNum ? "0.00" : "Enter value"}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          style={{
            flex: 1,
            padding: "8px 12px",
            background: C.bgInput,
            border: "none",
            outline: "none",
            fontSize: 13,
            color: C.text,
            fontFamily: "inherit",
            minWidth: 0,
          }}
        />
      </div>

      {/* Remove */}
      <button
        type="button"
        onClick={onRemove}
        disabled={isOnly}
        style={{
          width: compact ? "100%" : 36,
          height: 36,
          border: `1px solid ${C.border}`,
          background: "transparent",
          borderRadius: 6,
          cursor: isOnly ? "not-allowed" : "pointer",
          color: C.muted,
          fontSize: 16,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          opacity: isOnly ? 0.3 : 1,
          transition: "all .15s",
        }}
        onMouseEnter={(e) => {
          if (!isOnly) {
            e.currentTarget.style.background = "rgba(239,68,68,.1)";
            e.currentTarget.style.borderColor = C.danger;
            e.currentTarget.style.color = C.danger;
          }
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = "transparent";
          e.currentTarget.style.borderColor = C.border;
          e.currentTarget.style.color = C.muted;
        }}
      >
        ×
      </button>
    </div>
  );
}

// ── SEO section ────────────────────────────────────────────────────────────
function SeoSection({ collectionTitle, handle }) {
  const [open, setOpen] = useState(false);
  const [seoTitle, setSeoTitle] = useState("");
  const [seoDesc, setSeoDesc] = useState("");
  const [seoHandle, setSeoHandle] = useState("");

  useEffect(() => {
    if (!seoHandle && handle) setSeoHandle(handle);
  }, [handle]);

  useEffect(() => {
    if (!seoHandle && collectionTitle) {
      setSeoHandle(
        collectionTitle
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/^-|-$/g, ""),
      );
    }
  }, [collectionTitle]);

  const previewTitle = seoTitle || collectionTitle || "Collection title";
  const previewDesc =
    seoDesc ||
    "Add a description to see how this collection appears in search results.";
  const previewHandle = seoHandle || "collection-handle";

  return (
    <Card>
      <CardTitle
        right={
          <button
            type="button"
            onClick={() => setOpen((o) => !o)}
            style={{
              background: "transparent",
              border: "none",
              color: C.accent,
              fontSize: 12,
              cursor: "pointer",
              fontWeight: 600,
            }}
          >
            {open ? "Collapse" : "Edit"}
          </button>
        }
      >
        Search engine listing
      </CardTitle>
      <CardBody>
        <div style={{ padding: "4px 0 10px" }}>
          <div
            style={{
              fontSize: 16,
              color: "#5b9cf6",
              marginBottom: 2,
              wordBreak: "break-word",
            }}
          >
            {previewTitle}
          </div>
          <div style={{ fontSize: 12, color: "#4ade80", marginBottom: 4 }}>
            your-store.myshopify.com/collections/{previewHandle}
          </div>
          <div style={{ fontSize: 12, color: C.muted }}>{previewDesc}</div>
        </div>
        {open && (
          <>
            <div
              style={{ borderTop: `1px solid ${C.border}`, margin: "12px 0" }}
            />
            <Field
              label="Page title"
              help={`${seoTitle.length} of 70 characters used`}
            >
              <Input
                value={seoTitle}
                onChange={(e) => setSeoTitle(e.target.value)}
                placeholder="e.g. Summer Collection – Your Store"
              />
            </Field>
            <Field
              label="Meta description"
              optional
              help={`${seoDesc.length} of 320 characters used`}
            >
              <Textarea
                value={seoDesc}
                onChange={(e) => setSeoDesc(e.target.value)}
                placeholder="Describe this collection for search engines..."
                minHeight={80}
              />
            </Field>
            <Field label="URL handle" style={{ marginBottom: 0 }}>
              <Input
                value={seoHandle}
                onChange={(e) => setSeoHandle(e.target.value)}
                placeholder="collection-handle"
                prefix="/collections/"
              />
            </Field>
          </>
        )}
      </CardBody>
    </Card>
  );
}

// ── Products in collection (read-only list) ────────────────────────────────
function ProductsList({ products }) {
  if (!products || products.length === 0) {
    return (
      <div
        style={{
          fontSize: 13,
          color: C.muted,
          fontStyle: "italic",
          padding: "8px 0",
        }}
      >
        No products in this collection.
      </div>
    );
  }
  return products.map((p, i) => (
    <div
      key={p.id || i}
      style={{
        display: "flex",
        alignItems: "center",
        gap: 12,
        padding: "10px 0",
        borderBottom:
          i < products.length - 1 ? `1px solid ${C.border}` : "none",
      }}
    >
      <div
        style={{
          width: 40,
          height: 40,
          borderRadius: 6,
          border: `1px solid ${C.border}`,
          overflow: "hidden",
          flexShrink: 0,
          background: C.bgInput,
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
        }}
      >
        {p.images?.[0]?.src || p.image ? (
          <img
            src={p.images?.[0]?.src || p.image}
            alt={p.title}
            style={{ width: "100%", height: "100%", objectFit: "cover" }}
          />
        ) : (
          <span style={{ fontSize: 16 }}>📦</span>
        )}
      </div>
      <div style={{ flex: 1, minWidth: 0 }}>
        <div
          style={{
            fontSize: 13,
            fontWeight: 500,
            color: C.text,
            whiteSpace: "nowrap",
            overflow: "hidden",
            textOverflow: "ellipsis",
          }}
        >
          {p.title}
        </div>
        <div style={{ fontSize: 11, color: C.muted, marginTop: 1 }}>
          {(p.status || "draft").charAt(0).toUpperCase() +
            (p.status || "draft").slice(1)}
        </div>
      </div>
    </div>
  ));
}

function ProductPicker({
  products,
  selectedProductIds,
  setSelectedProductIds,
  search,
  setSearch,
  disabled,
}) {
  const selectedSet = useMemo(
    () => new Set(selectedProductIds.map((id) => Number(id))),
    [selectedProductIds],
  );

  const filteredProducts = useMemo(() => {
    const query = search.trim().toLowerCase();
    const base = query
      ? products.filter((product) => {
          const tags = Array.isArray(product.tags)
            ? product.tags.join(", ")
            : String(product.tags || "");
          return [product.title, product.vendor, product.product_type, tags]
            .filter(Boolean)
            .some((value) => String(value).toLowerCase().includes(query));
        })
      : products;
    return base.slice(0, 12);
  }, [products, search]);

  const toggleProduct = (productId) => {
    const normalizedId = Number(productId);
    setSelectedProductIds((prev) => {
      const exists = prev.some((id) => Number(id) === normalizedId);
      if (exists) {
        return prev.filter((id) => Number(id) !== normalizedId);
      }
      return [...prev, normalizedId];
    });
  };

  return (
    <>
      <Field label="Search products" style={{ marginBottom: 12 }}>
        <Input
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          placeholder="Search by title, vendor, type, or tag"
        />
      </Field>
      <div
        style={{
          display: "grid",
          gap: 8,
          maxHeight: 320,
          overflowY: "auto",
          paddingRight: 4,
        }}
      >
        {filteredProducts.map((product) => {
          const firstImage =
            product.image ||
            product.images?.[0]?.src ||
            product.images?.[0]?.url;
          const checked = selectedSet.has(Number(product.id));
          return (
            <button
              key={product.id}
              type="button"
              disabled={disabled}
              onClick={() => toggleProduct(product.id)}
              style={{
                display: "flex",
                alignItems: "center",
                gap: 10,
                width: "100%",
                textAlign: "left",
                background: checked ? "var(--accent-light)" : C.bgCard,
                border: `1px solid ${checked ? C.accent : C.border}`,
                borderRadius: 8,
                padding: "10px 12px",
                cursor: disabled ? "not-allowed" : "pointer",
                transition: "all .15s",
                opacity: disabled ? 0.6 : 1,
              }}
            >
              <div
                style={{
                  width: 16,
                  height: 16,
                  borderRadius: 4,
                  border: `1px solid ${checked ? C.accent : C.border}`,
                  background: checked ? C.accent : "transparent",
                  color: "#fff",
                  fontSize: 11,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  flexShrink: 0,
                }}
              >
                {checked ? "✓" : ""}
              </div>
              <div
                style={{
                  width: 36,
                  height: 36,
                  borderRadius: 6,
                  overflow: "hidden",
                  border: `1px solid ${C.border}`,
                  background: C.bgInput,
                  display: "flex",
                  alignItems: "center",
                  justifyContent: "center",
                  flexShrink: 0,
                }}
              >
                {firstImage ? (
                  <img
                    src={firstImage}
                    alt=""
                    style={{
                      width: "100%",
                      height: "100%",
                      objectFit: "cover",
                    }}
                  />
                ) : (
                  <span style={{ fontSize: 14 }}>📦</span>
                )}
              </div>
              <div style={{ flex: 1, minWidth: 0 }}>
                <div
                  style={{
                    fontSize: 13,
                    fontWeight: 500,
                    color: C.text,
                    whiteSpace: "nowrap",
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                  }}
                >
                  {product.title}
                </div>
                <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>
                  {product.vendor ||
                    product.product_type ||
                    product.status ||
                    "Product"}
                </div>
              </div>
            </button>
          );
        })}
        {!filteredProducts.length && (
          <div style={{ fontSize: 12, color: C.muted, padding: "6px 0" }}>
            No products match your search.
          </div>
        )}
      </div>
    </>
  );
}

// ── Main AddCollectionPage ─────────────────────────────────────────────────
export default function AddCollectionPage({ toast, onBack, editCollection }) {
  const pickCollectionPayload = (payload) => {
    if (!payload) return null;
    const base =
      payload.custom_collection ||
      payload.smart_collection ||
      payload.collection ||
      payload;
    if (!base || !base.id) return null;
    return {
      ...base,
      collection_type:
        base.collection_type ||
        (payload.smart_collection
          ? "smart"
          : payload.custom_collection
            ? "custom"
            : "custom"),
    };
  };

  const [currentCollection, setCurrentCollection] = useState(
    editCollection || null,
  );
  const isEdit = !!currentCollection;
  const viewportWidth = useViewportWidth();
  const isTabletOrBelow = viewportWidth <= 1024;
  const isPhone = viewportWidth <= 640;

  const [title, setTitle] = useState(editCollection?.title || "");
  const [collectionType, setCollectionType] = useState(
    editCollection?.collection_type || "custom",
  );
  const [status, setStatus] = useState(
    editCollection?.published_at ? "active" : "draft",
  );
  const [imageFile, setImageFile] = useState(null);
  const [imageUrl, setImageUrl] = useState(editCollection?.image?.src || "");
  const [removeImg, setRemoveImg] = useState(false);
  const [saving, setSaving] = useState(false);
  const [loadingDetails, setLoadingDetails] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [conditions, setConditions] = useState([newCond()]);
  const [matchType, setMatchType] = useState("all");
  const [products, setProducts] = useState([]);
  const [productOptions, setProductOptions] = useState([]);
  const [productsLoading, setProductsLoading] = useState(false);
  const [productSearch, setProductSearch] = useState("");
  const [selectedProductIds, setSelectedProductIds] = useState([]);
  const descRef = useRef(null);
  const isBusy = saving || deleting || loadingDetails;
  const overlayState = loadingDetails
    ? {
        badge: "LOADING COLLECTION",
        title: "Loading collection details",
        subtitle:
          "Fetching the latest collection data so the editor opens with the current Shopify values.",
      }
    : saving
      ? {
          badge: isEdit ? "SAVING COLLECTION" : "CREATING COLLECTION",
          title: isEdit ? "Saving collection changes" : "Creating collection",
          subtitle:
            "Updating collection details, products, and rules in Shopify.",
        }
      : deleting
        ? {
            badge: "DELETING COLLECTION",
            title: "Deleting collection",
            subtitle:
              "Removing the collection from Shopify and cleaning up the editor state.",
          }
        : null;

  const isSmart = collectionType === "smart";

  useEffect(() => {
    const nextCollection = editCollection || null;
    setCurrentCollection(nextCollection);
    setTitle(nextCollection?.title || "");
    setCollectionType(nextCollection?.collection_type || "custom");
    setStatus(nextCollection?.published_at ? "active" : "draft");
    setImageUrl(nextCollection?.image?.src || "");
    setImageFile(null);
    setRemoveImg(false);
    setProducts([]);
    setSelectedProductIds([]);
    setProductSearch("");
    setConditions([newCond()]);
    setMatchType("all");

    if (descRef.current) {
      descRef.current.innerHTML = nextCollection?.body_html || "";
    }
  }, [editCollection?.id]);

  // Load products for existing collection
  useEffect(() => {
    if (isEdit && currentCollection?.id) {
      setLoadingDetails(true);
      api
        .get(`/collections/${currentCollection.id}`, { force: true })
        .then((d) => {
          const collectionData = pickCollectionPayload(d);
          if (collectionData?.id) {
            setCurrentCollection((prev) => ({ ...prev, ...collectionData }));
            setTitle(collectionData.title || "");
            setCollectionType(collectionData.collection_type || "custom");
            setImageUrl(collectionData.image?.src || "");
            setStatus(collectionData.published_at ? "active" : "draft");
            if (descRef.current) {
              descRef.current.innerHTML = collectionData.body_html || "";
            }
          }
          if (d.products) {
            setProducts(d.products);
            setSelectedProductIds(
              d.products.map((product) => Number(product.id)),
            );
          }
          if (d.rules?.length) {
            setConditions(
              d.rules.map((r) => ({
                _id: ++_condId,
                field: r.column,
                operator: r.relation,
                value: r.condition,
              })),
            );
            setMatchType(d.disjunctive ? "any" : "all");
          }
        })
        .catch(() => {})
        .finally(() => setLoadingDetails(false));
    }
  }, [isEdit, currentCollection?.id]);

  useEffect(() => {
    let mounted = true;

    const loadProducts = async () => {
      setProductsLoading(true);
      try {
        const response = await api.get("/products?fetch_all=true");
        if (!mounted) return;
        setProductOptions(response?.products || []);
      } catch {
        if (mounted) {
          setProductOptions([]);
        }
      } finally {
        if (mounted) {
          setProductsLoading(false);
        }
      }
    };

    loadProducts();

    return () => {
      mounted = false;
    };
  }, []);

  function updateCond(id, field, val) {
    setConditions((prev) =>
      prev.map((c) => {
        if (c._id !== id) return c;
        // Reset operator if field type changes
        const wasNum = NUMERIC_FIELDS.has(c.field);
        const isNum = NUMERIC_FIELDS.has(field === "field" ? val : c.field);
        if (field === "field" && wasNum !== isNum) {
          return {
            ...c,
            field: val,
            operator: (isNum ? NUM_OPS : TEXT_OPS)[0].v,
            value: "",
          };
        }
        return { ...c, [field]: val };
      }),
    );
  }

  const productMap = useMemo(
    () =>
      new Map(productOptions.map((product) => [Number(product.id), product])),
    [productOptions],
  );

  const selectedProducts = useMemo(() => {
    const normalizedIds = selectedProductIds.map((id) => Number(id));
    return normalizedIds
      .map(
        (id) =>
          productMap.get(id) ||
          products.find((product) => Number(product.id) === id),
      )
      .filter(Boolean);
  }, [productMap, products, selectedProductIds]);

  const matchesText = (actualValue, operator, expectedValue) => {
    const actual = String(actualValue || "").toLowerCase();
    const expected = String(expectedValue || "").toLowerCase();
    switch (operator) {
      case "equals":
        return actual === expected;
      case "not_equals":
        return actual !== expected;
      case "starts_with":
        return actual.startsWith(expected);
      case "ends_with":
        return actual.endsWith(expected);
      case "contains":
        return actual.includes(expected);
      case "not_contains":
        return !actual.includes(expected);
      default:
        return false;
    }
  };

  const matchesNumeric = (actualValue, operator, expectedValue) => {
    const actual = Number(actualValue ?? 0);
    const expected = Number(expectedValue ?? 0);
    if (Number.isNaN(expected)) return false;
    switch (operator) {
      case "greater_than":
        return actual > expected;
      case "less_than":
        return actual < expected;
      case "equals":
        return actual === expected;
      case "not_equals":
        return actual !== expected;
      default:
        return false;
    }
  };

  const doesProductMatchCondition = (product, condition) => {
    const variants = Array.isArray(product.variants) ? product.variants : [];
    switch (condition.field) {
      case "title":
        return matchesText(product.title, condition.operator, condition.value);
      case "type":
        return matchesText(
          product.product_type,
          condition.operator,
          condition.value,
        );
      case "vendor":
        return matchesText(product.vendor, condition.operator, condition.value);
      case "tag": {
        const tags = Array.isArray(product.tags)
          ? product.tags.join(", ")
          : String(product.tags || "");
        return matchesText(tags, condition.operator, condition.value);
      }
      case "variant_price":
        return variants.some((variant) =>
          matchesNumeric(variant.price, condition.operator, condition.value),
        );
      case "variant_weight":
        return variants.some((variant) =>
          matchesNumeric(variant.weight, condition.operator, condition.value),
        );
      case "variant_inventory":
        return variants.some((variant) =>
          matchesNumeric(
            variant.inventory_quantity,
            condition.operator,
            condition.value,
          ),
        );
      default:
        return false;
    }
  };

  const filledConds = conditions.filter((c) => c.value.trim() !== "");

  const smartPreviewProducts = useMemo(() => {
    if (!isSmart || !filledConds.length) return [];
    return productOptions.filter((product) => {
      const results = filledConds.map((condition) =>
        doesProductMatchCondition(product, condition),
      );
      return matchType === "all"
        ? results.every(Boolean)
        : results.some(Boolean);
    });
  }, [filledConds, isSmart, matchType, productOptions]);

  async function handleSave() {
    if (!title.trim()) {
      toast("Title is required", "error");
      return;
    }
    if (isSmart && !filledConds.length) {
      toast("Add at least one smart collection condition", "error");
      return;
    }
    setSaving(true);
    try {
      const payload = {
        title: title.trim(),
        body_html: descRef.current?.innerHTML || "",
        collection_type: collectionType,
        published_at: status === "active" ? new Date().toISOString() : null,
      };

      if (isSmart) {
        payload.disjunctive = matchType === "any";
        payload.rules = filledConds.map((condition) => ({
          column: condition.field,
          relation: condition.operator,
          condition: condition.value.trim(),
        }));
      }

      // Image
      if (imageFile) {
        const b64 = await new Promise((res, rej) => {
          const reader = new FileReader();
          reader.onload = (e) => {
            const r = String(e.target.result || "");
            const idx = r.indexOf(",");
            res(idx >= 0 ? r.slice(idx + 1) : r);
          };
          reader.onerror = rej;
          reader.readAsDataURL(imageFile);
        });
        payload.image = { attachment: b64, filename: imageFile.name };
      } else if (removeImg) {
        payload.image = null;
      }

      if (isEdit) {
        const response = await api.put(
          `/collections/${currentCollection.id}`,
          payload,
        );
        const savedCollection = pickCollectionPayload(response);
        if (savedCollection?.id) {
          setCurrentCollection((prev) => ({ ...prev, ...savedCollection }));
          setTitle(savedCollection.title || title);
          setCollectionType(savedCollection.collection_type || collectionType);
          setImageUrl(savedCollection.image?.src || imageUrl);
          setStatus(savedCollection.published_at ? "active" : "draft");
        }
        if (!isSmart) {
          await api.put(
            `/collections/${currentCollection.id}/products`,
            selectedProductIds,
          );
          setProducts(selectedProducts);
        }
        toast("Collection updated!");
      } else {
        const response = await api.post("/collections/", payload);
        const savedCollection = pickCollectionPayload(response);
        if (savedCollection?.id) {
          setCurrentCollection(savedCollection);
          setTitle(savedCollection.title || title);
          setCollectionType(savedCollection.collection_type || collectionType);
          setImageUrl(savedCollection.image?.src || imageUrl);
          setStatus(savedCollection.published_at ? "active" : status);
          if (!isSmart) {
            await api.put(
              `/collections/${savedCollection.id}/products`,
              selectedProductIds,
            );
            setProducts(selectedProducts);
          }
        }
        toast("Collection created!");
      }
    } catch (e) {
      toast(e?.message || "Failed to save collection", "error");
    }
    setSaving(false);
  }

  async function handleDelete() {
    if (!window.confirm(`Delete "${title}"? This cannot be undone.`)) return;
    setDeleting(true);
    try {
      await api.delete(`/collections/${currentCollection.id}`);
      toast("Collection deleted");
      onBack();
    } catch {
      toast("Failed to delete", "error");
    }
    setDeleting(false);
  }

  return (
    <div
      style={{
        maxWidth: 1100,
        margin: "0 auto",
        padding: isPhone ? "0 14px 120px" : "0 24px 120px",
        fontFamily: "inherit",
        position: "relative",
        minHeight: "calc(100vh - 120px)",
      }}
    >
      {isBusy && overlayState && <PageLoadingOverlay {...overlayState} />}

      {/* Breadcrumb */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 6,
          flexWrap: "wrap",
          fontSize: 12,
          color: C.muted,
          marginBottom: 14,
        }}
      >
        <button
          type="button"
          onClick={onBack}
          style={{
            background: "none",
            border: "none",
            color: C.muted,
            cursor: "pointer",
            fontSize: 12,
            padding: 0,
            fontFamily: "inherit",
          }}
        >
          Collections
        </button>
        <span style={{ color: C.disabled }}>›</span>
        <span style={{ color: C.text }}>
          {isEdit ? title || "Edit collection" : "Add collection"}
        </span>
      </div>

      {/* Page header */}
      <div
        style={{
          display: "flex",
          alignItems: isPhone ? "flex-start" : "center",
          justifyContent: "space-between",
          marginBottom: 20,
          gap: 12,
          flexWrap: "wrap",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: 10,
            flexWrap: "wrap",
          }}
        >
          <Btn
            onClick={onBack}
            variant="secondary"
            disabled={isBusy}
            style={{ padding: "8px 12px" }}
          >
            ← Back
          </Btn>
          <h1
            style={{ fontSize: 20, fontWeight: 700, color: C.text, margin: 0 }}
          >
            {isEdit ? title || "Edit collection" : "Add collection"}
          </h1>
        </div>
        <div
          style={{
            display: "flex",
            gap: 8,
            flexWrap: "wrap",
            width: isPhone ? "100%" : "auto",
          }}
        >
          <Btn onClick={onBack} variant="secondary" disabled={isBusy}>
            Discard
          </Btn>
          {isEdit && (
            <Btn onClick={handleDelete} disabled={isBusy} variant="danger">
              {deleting ? (
                <>
                  <Spin size={14} /> Deleting…
                </>
              ) : (
                "🗑 Delete"
              )}
            </Btn>
          )}
          <Btn onClick={handleSave} disabled={isBusy} variant="primary">
            {saving ? (
              <>
                <Spin size={14} /> Saving…
              </>
            ) : loadingDetails ? (
              <>
                <Spin size={14} /> Loading…
              </>
            ) : isEdit ? (
              "Save changes"
            ) : (
              "Save"
            )}
          </Btn>
        </div>
      </div>

      {/* Two-column grid */}
      <div
        style={{
          display: "grid",
          gridTemplateColumns: isTabletOrBelow ? "1fr" : "1fr 270px",
          gap: 16,
          alignItems: "start",
        }}
      >
        {/* ── LEFT ── */}
        <div>
          {/* Title & Description */}
          <Card>
            <CardBody>
              <Field label="Title">
                <Input
                  value={title}
                  onChange={(e) => setTitle(e.target.value)}
                  placeholder="e.g. Summer Collection"
                />
              </Field>
              <Field label="Description" optional style={{ marginBottom: 0 }}>
                <RichText
                  valueRef={descRef}
                  initialHtml={currentCollection?.body_html || ""}
                />
              </Field>
            </CardBody>
          </Card>

          <Card>
            <CardTitle>Collection type</CardTitle>
            <CardBody>
              <CollectionTypeSwitch
                value={collectionType}
                onChange={setCollectionType}
                disabled={isBusy}
              />
            </CardBody>
          </Card>

          {/* Conditions — shown for smart collections when editing, always shown for new */}
          {isSmart && (
            <Card>
              <CardTitle>Conditions</CardTitle>
              <CardBody>
                <div
                  style={{
                    display: "flex",
                    alignItems: "center",
                    gap: 16,
                    flexWrap: "wrap",
                    marginBottom: 16,
                    fontSize: 13,
                  }}
                >
                  <span style={{ fontWeight: 500, color: C.text }}>
                    Products must match:
                  </span>
                  {["all", "any"].map((v) => (
                    <label
                      key={v}
                      style={{
                        display: "flex",
                        alignItems: "center",
                        gap: 6,
                        cursor: "pointer",
                        color: C.text,
                        fontSize: 13,
                      }}
                    >
                      <input
                        type="radio"
                        name="matchType"
                        value={v}
                        checked={matchType === v}
                        onChange={() => setMatchType(v)}
                        style={{ accentColor: C.accent, width: 16, height: 16 }}
                      />
                      {v} conditions
                    </label>
                  ))}
                </div>

                {conditions.map((c) => (
                  <ConditionRow
                    key={c._id}
                    cond={c}
                    compact={isPhone}
                    onChange={(f, v) => updateCond(c._id, f, v)}
                    onRemove={() =>
                      setConditions((prev) => {
                        const next = prev.filter((x) => x._id !== c._id);
                        return next.length ? next : [newCond()];
                      })
                    }
                    isOnly={conditions.length === 1}
                  />
                ))}

                {filledConds.length > 0 && (
                  <div
                    style={{
                      display: "flex",
                      gap: 6,
                      alignItems: "flex-start",
                      fontSize: 12,
                      color: "#fbbf24",
                      background: "rgba(251,191,36,.08)",
                      border: "1px solid rgba(251,191,36,.2)",
                      borderRadius: 7,
                      padding: "8px 12px",
                      margin: "12px 0",
                    }}
                  >
                    <span>ⓘ</span>
                    <span>
                      This collection will include all products matching{" "}
                      {matchType === "all" ? "all" : "any"} of:{" "}
                      {[
                        ...new Set(
                          filledConds.map(
                            (c) =>
                              COND_FIELDS.find((f) => f.value === c.field)
                                ?.label,
                          ),
                        ),
                      ].join(", ")}
                      .
                    </span>
                  </div>
                )}

                <Btn
                  onClick={() => setConditions((p) => [...p, newCond()])}
                  variant="secondary"
                  style={{ marginTop: 4, fontSize: 12 }}
                >
                  + Add another condition
                </Btn>
              </CardBody>
            </Card>
          )}

          <Card>
            <CardTitle
              right={
                <span style={{ fontSize: 12, fontWeight: 400, color: C.muted }}>
                  {isSmart
                    ? `${smartPreviewProducts.length} matching`
                    : `${selectedProducts.length} selected`}
                </span>
              }
            >
              Products
            </CardTitle>
            <CardBody>
              {isSmart ? (
                <>
                  <p style={{ fontSize: 12, color: C.muted, marginBottom: 12 }}>
                    {filledConds.length
                      ? "Live preview of products matching the current smart rules."
                      : "Add conditions above to preview which products will be included."}
                  </p>
                  <ProductsList products={smartPreviewProducts} />
                </>
              ) : (
                <>
                  <p style={{ fontSize: 12, color: C.muted, marginBottom: 12 }}>
                    Choose the products that belong to this manual collection.
                  </p>
                  {productsLoading ? (
                    <div style={{ fontSize: 12, color: C.muted }}>
                      Loading products…
                    </div>
                  ) : (
                    <ProductPicker
                      products={productOptions}
                      selectedProductIds={selectedProductIds}
                      setSelectedProductIds={setSelectedProductIds}
                      search={productSearch}
                      setSearch={setProductSearch}
                      disabled={isBusy}
                    />
                  )}
                  <div
                    style={{
                      borderTop: `1px solid ${C.border}`,
                      margin: "14px 0 10px",
                    }}
                  />
                  <ProductsList products={selectedProducts} />
                </>
              )}
            </CardBody>
          </Card>

          {isEdit && editCollection?.id && (
            <Card>
              <CardTitle>Metafields</CardTitle>
              <CardBody>
                <MetafieldsPanel
                  resource="collections"
                  resourceId={editCollection.id}
                  resourceName={title}
                  toast={toast}
                />
              </CardBody>
            </Card>
          )}

          {/* SEO */}
          <SeoSection
            collectionTitle={title}
            handle={currentCollection?.handle}
          />
        </div>

        {/* ── RIGHT ── */}
        <div>
          {/* Visibility */}
          <Card>
            <CardTitle>Visibility</CardTitle>
            <CardBody>
              <Field style={{ marginBottom: 10 }}>
                <Select
                  value={status}
                  onChange={(e) => setStatus(e.target.value)}
                  options={[
                    { v: "active", l: "Active" },
                    { v: "draft", l: "Draft" },
                  ]}
                />
              </Field>
              <StatusDot status={status} />
              <p style={{ fontSize: 11, color: C.muted, marginTop: 6 }}>
                {status === "active"
                  ? "This collection is visible in your online store."
                  : "This collection is hidden from your online store."}
              </p>
            </CardBody>
          </Card>

          {/* Collection image */}
          <CollectionImageCard
            imageUrl={removeImg ? null : imageUrl}
            imageFile={imageFile}
            onFileChange={(f) => {
              setImageFile(f);
              setRemoveImg(false);
            }}
            onRemove={() => {
              setImageFile(null);
              setImageUrl("");
              setRemoveImg(true);
            }}
          />

          {/* Collection details (edit only) */}
          {isEdit && currentCollection && (
            <Card>
              <CardTitle>Collection details</CardTitle>
              <CardBody>
                <div style={{ fontSize: 12, color: C.muted, marginBottom: 4 }}>
                  Handle
                </div>
                <div
                  style={{
                    fontSize: 12,
                    color: C.text,
                    fontFamily: "monospace",
                    wordBreak: "break-all",
                    marginBottom: 14,
                  }}
                >
                  {currentCollection.handle || "—"}
                </div>
                <div
                  style={{
                    borderTop: `1px solid ${C.border}`,
                    marginBottom: 12,
                  }}
                />
                <div style={{ fontSize: 12, color: C.muted, marginBottom: 4 }}>
                  Collection type
                </div>
                <div style={{ fontSize: 12, color: C.text, marginBottom: 14 }}>
                  {currentCollection.collection_type
                    ? collectionType.charAt(0).toUpperCase() +
                      collectionType.slice(1)
                    : "Custom"}
                </div>
                <div
                  style={{
                    borderTop: `1px solid ${C.border}`,
                    marginBottom: 12,
                  }}
                />
                <div style={{ fontSize: 12, color: C.muted, marginBottom: 4 }}>
                  Collection ID
                </div>
                <div
                  style={{
                    fontSize: 11,
                    color: C.disabled,
                    fontFamily: "monospace",
                    wordBreak: "break-all",
                  }}
                >
                  {currentCollection.id}
                </div>
              </CardBody>
            </Card>
          )}
        </div>
      </div>

      {/* Sticky save bar */}
      <div
        style={{
          position: "fixed",
          bottom: 0,
          left: isTabletOrBelow ? 0 : 220,
          right: 0,
          background: C.card,
          borderTop: `1px solid ${C.border}`,
          padding: isPhone ? "10px 12px" : "12px 24px",
          display: "flex",
          alignItems: isPhone ? "stretch" : "center",
          flexDirection: isPhone ? "column" : "row",
          justifyContent: "space-between",
          gap: isPhone ? 8 : 0,
          zIndex: 200,
        }}
      >
        <span style={{ fontSize: 13, color: C.muted }}>
          {saving
            ? "Saving…"
            : isEdit
              ? "Unsaved changes"
              : "Unsaved collection"}
        </span>
        <div
          style={{ display: "flex", gap: 8, width: isPhone ? "100%" : "auto" }}
        >
          <Btn onClick={onBack} variant="secondary">
            Discard
          </Btn>
          <Btn
            onClick={handleSave}
            disabled={isBusy}
            variant="primary"
            style={isPhone ? { flex: 1, justifyContent: "center" } : {}}
          >
            {saving ? (
              <>
                <Spin size={14} /> Saving…
              </>
            ) : loadingDetails ? (
              <>
                <Spin size={14} /> Loading…
              </>
            ) : isEdit ? (
              "Save changes"
            ) : (
              "Save"
            )}
          </Btn>
        </div>
      </div>
    </div>
  );
}
