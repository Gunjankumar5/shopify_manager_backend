import { useState, useEffect, useCallback } from "react";
import { API_BASE_URL } from "../api/config";
import { api } from "../api/api";
import { Spin } from "./Icons";

const C = {
  card: "var(--bg-card)",
  border: "var(--border-strong)",
  borderFocus: "var(--accent)",
  text: "var(--text-primary)",
  muted: "var(--text-muted)",
  disabled: "var(--text-muted)",
  accent: "var(--accent)",
  success: "var(--success)",
  danger: "var(--danger)",
  warning: "var(--warning)",
};

const METAFIELD_TYPE_GROUPS = [
  {
    group: "Text",
    types: [
      { value: "single_line_text_field", label: "Single line text" },
      { value: "multi_line_text_field", label: "Multi line text" },
      { value: "rich_text_field", label: "Rich text (HTML)" },
    ],
  },
  {
    group: "Number",
    types: [
      { value: "number_integer", label: "Integer" },
      { value: "number_decimal", label: "Decimal" },
      { value: "rating", label: "Rating" },
    ],
  },
  {
    group: "Date & Time",
    types: [
      { value: "date", label: "Date" },
      { value: "date_time", label: "Date & Time" },
    ],
  },
  {
    group: "Other",
    types: [
      { value: "boolean", label: "True / False" },
      { value: "color", label: "Color" },
      { value: "url", label: "URL" },
      { value: "json", label: "JSON" },
    ],
  },
  {
    group: "Measurements",
    types: [
      { value: "dimension", label: "Dimension" },
      { value: "weight", label: "Weight" },
      { value: "volume", label: "Volume" },
      { value: "money", label: "Money" },
    ],
  },
  {
    group: "References",
    types: [
      { value: "page_reference", label: "Page" },
      { value: "product_reference", label: "Product" },
      { value: "collection_reference", label: "Collection" },
      { value: "variant_reference", label: "Variant" },
      { value: "file_reference", label: "File" },
    ],
  },
  {
    group: "Lists",
    types: [
      { value: "list.single_line_text_field", label: "List: Text" },
      { value: "list.number_integer", label: "List: Integer" },
      { value: "list.color", label: "List: Colors" },
      { value: "list.date", label: "List: Dates" },
      { value: "list.date_time", label: "List: Date & Times" },
      { value: "list.url", label: "List: URLs" },
      { value: "list.product_reference", label: "List: Products" },
      { value: "list.collection_reference", label: "List: Collections" },
      { value: "list.variant_reference", label: "List: Variants" },
      { value: "list.file_reference", label: "List: Files" },
      { value: "list.page_reference", label: "List: Pages" },
    ],
  },
];

const TYPE_COLORS = {
  single_line_text_field: {
    bg: "var(--color-blue-light)",
    c: "var(--color-blue)",
  },
  multi_line_text_field: {
    bg: "var(--color-blue-light)",
    c: "var(--color-blue)",
  },
  rich_text_field: { bg: "var(--color-blue-light)", c: "var(--color-blue)" },
  number_integer: { bg: "var(--success-light)", c: "var(--success)" },
  number_decimal: { bg: "var(--success-light)", c: "var(--success)" },
  rating: { bg: "var(--warning-light)", c: "var(--warning)" },
  date: { bg: "var(--color-purple-light)", c: "var(--color-purple)" },
  date_time: { bg: "var(--color-purple-light)", c: "var(--color-purple)" },
  boolean: { bg: "var(--warning-light)", c: "var(--warning)" },
  color: { bg: "var(--color-red-light)", c: "var(--color-red)" },
  url: { bg: "var(--bg-overlay-light)", c: "var(--text-muted)" },
  json: { bg: "var(--bg-overlay-light)", c: "var(--text-muted)" },
  dimension: { bg: "var(--color-cyan-light)", c: "var(--color-cyan)" },
  weight: { bg: "var(--color-cyan-light)", c: "var(--color-cyan)" },
  volume: { bg: "var(--color-cyan-light)", c: "var(--color-cyan)" },
  money: { bg: "var(--color-cyan-light)", c: "var(--color-cyan)" },
};

function getTypeColor(type) {
  if (!type) return { bg: "var(--bg-overlay-light)", c: "var(--text-muted)" };
  if (type.startsWith("list."))
    return { bg: "var(--accent-light)", c: "var(--accent)" };
  if (type.endsWith("_reference"))
    return { bg: "var(--color-pink-light)", c: "var(--color-pink)" };
  return (
    TYPE_COLORS[type] || {
      bg: "var(--bg-overlay-light)",
      c: "var(--text-muted)",
    }
  );
}

function getTypeLabel(type) {
  for (const group of METAFIELD_TYPE_GROUPS) {
    const found = group.types.find((t) => t.value === type);
    if (found) return found.label;
  }
  return type || "—";
}

function formatForDisplay(value) {
  if (value === null || value === undefined || value === "") return null;
  const str = typeof value === "object" ? JSON.stringify(value) : String(value);
  return str.length > 120 ? str.slice(0, 120) + "…" : str;
}

function formatForSave(value, type) {
  if (["dimension", "weight", "volume", "money"].includes(type)) {
    try {
      return JSON.stringify(
        typeof value === "string" ? JSON.parse(value) : value,
      );
    } catch {
      return value;
    }
  }
  if (type?.startsWith("list.")) {
    try {
      const parsed = JSON.parse(value);
      return JSON.stringify(Array.isArray(parsed) ? parsed : [value]);
    } catch {
      return JSON.stringify([value]);
    }
  }
  return String(value ?? "");
}

const baseInput = {
  width: "100%",
  padding: "6px 9px",
  background: "var(--bg-input)",
  border: `1px solid ${C.border}`,
  borderRadius: 5.25,
  fontSize: 9.75,
  color: C.text,
  outline: "none",
  fontFamily: "inherit",
  transition: "border-color .15s, box-shadow .15s",
  boxSizing: "border-box",
};

// ── Extract options from validations ───────────────────────────────────────────
function extractOptionsFromValidations(validations = []) {
  if (!Array.isArray(validations)) return null;

  const choicesValidation = validations.find((v) => v.name === "choices");
  if (!choicesValidation) return null;

  // Value is typically a JSON string array or direct array
  let options = choicesValidation.value;
  if (typeof options === "string") {
    try {
      options = JSON.parse(options);
    } catch {
      return null;
    }
  }

  return Array.isArray(options) && options.length > 0 ? options : null;
}

// ── Smart value input per type ─────────────────────────────────────────────────
function ValueInput({
  type,
  value,
  onChange,
  placeholder,
  options,
  onInputBlur,
  autoFocus,
}) {
  const [focused, setFocused] = useState(false);
  const style = {
    ...baseInput,
    ...(focused
      ? {
          borderColor: C.borderFocus,
          boxShadow: "var(--focus-ring)",
        }
      : {}),
  };
  const onFocus = () => setFocused(true);
  const onBlur = () => {
    onInputBlur?.();
    setFocused(false);
  };

  // Render dropdown if options exist
  if (options && Array.isArray(options) && options.length > 0) {
    return (
      <select
        value={value || ""}
        onChange={(e) => onChange(e.target.value)}
        style={{
          ...style,
          appearance: "none",
          cursor: "pointer",
          paddingRight: 32,
          backgroundImage: `url('data:image/svg+xml;charset=utf-8,%3Csvg xmlns=%22http://www.w3.org/2000/svg%22 width=%2212%22 height=%2212%22 viewBox=%220 0 24 24%22 fill=%22none%22 stroke=%22${encodeURIComponent(C.muted)}%22 stroke-width=%222%22%3E%3Cpath d=%22M6 9l6 6 6-6%22/%3E%3C/svg%3E')`,
          backgroundRepeat: "no-repeat",
          backgroundPosition: "right 10px center",
          backgroundSize: "12px",
        }}
        onFocus={onFocus}
        onBlur={onBlur}
        autoFocus={autoFocus}
      >
        <option value="">-- Select an option --</option>
        {options.map((opt, idx) => (
          <option key={idx} value={opt || ""}>
            {opt}
          </option>
        ))}
      </select>
    );
  }

  if (type === "boolean") {
    return (
      <select
        value={value || "true"}
        onChange={(e) => onChange(e.target.value)}
        style={style}
        onFocus={onFocus}
        onBlur={onBlur}
        autoFocus={autoFocus}
      >
        <option value="true">true</option>
        <option value="false">false</option>
      </select>
    );
  }
  if (["number_integer", "number_decimal", "rating"].includes(type)) {
    return (
      <input
        type="number"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        style={style}
        onFocus={onFocus}
        onBlur={onBlur}
        autoFocus={autoFocus}
        placeholder={placeholder}
      />
    );
  }
  if (type === "date") {
    return (
      <input
        type="date"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        style={style}
        onFocus={onFocus}
        onBlur={onBlur}
        autoFocus={autoFocus}
      />
    );
  }
  if (type === "date_time") {
    return (
      <input
        type="datetime-local"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        style={style}
        onFocus={onFocus}
        onBlur={onBlur}
        autoFocus={autoFocus}
      />
    );
  }
  if (type === "color") {
    return (
      <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
        <input
          type="color"
          value={value || "#000000"}
          onChange={(e) => onChange(e.target.value)}
          style={{
            width: 30,
            height: 28.5,
            borderRadius: 4.5,
            border: `1px solid ${C.border}`,
            cursor: "pointer",
            background: "transparent",
            padding: 1.5,
          }}
        />
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          style={{ ...style, flex: 1 }}
          placeholder="#000000"
          onFocus={onFocus}
          onBlur={onBlur}
        />
      </div>
    );
  }
  if (["json", "rich_text_field"].includes(type)) {
    return (
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onFocus={onFocus}
        onBlur={onBlur}
        style={{ ...style, minHeight: 67.5, resize: "vertical" }}
        placeholder={
          type === "json" ? '{"key": "value"}' : "<p>HTML content</p>"
        }
      />
    );
  }
  if (type === "multi_line_text_field") {
    return (
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onFocus={onFocus}
        onBlur={onBlur}
        style={{ ...style, minHeight: 60, resize: "vertical" }}
        placeholder={placeholder}
      />
    );
  }
  if (["dimension", "weight", "volume"].includes(type)) {
    let parsed = { value: "", unit: "" };
    try {
      const p = JSON.parse(value);
      if (p && typeof p === "object") parsed = p;
    } catch {}
    const hints = {
      dimension: "cm, mm, in, ft",
      weight: "kg, g, lb, oz",
      volume: "ml, L, fl_oz",
    };
    return (
      <div style={{ display: "flex", gap: 8 }}>
        <input
          type="number"
          value={parsed.value || ""}
          onChange={(e) =>
            onChange(JSON.stringify({ ...parsed, value: e.target.value }))
          }
          style={{ ...style, flex: 1 }}
          placeholder="Value"
        />
        <input
          type="text"
          value={parsed.unit || ""}
          onChange={(e) =>
            onChange(JSON.stringify({ ...parsed, unit: e.target.value }))
          }
          style={{ ...style, width: 110 }}
          placeholder={hints[type]}
        />
      </div>
    );
  }
  if (type === "money") {
    let parsed = { amount: "", currency_code: "USD" };
    try {
      const p = JSON.parse(value);
      if (p && typeof p === "object") parsed = p;
    } catch {}
    return (
      <div style={{ display: "flex", gap: 8 }}>
        <input
          type="number"
          value={parsed.amount || ""}
          onChange={(e) =>
            onChange(JSON.stringify({ ...parsed, amount: e.target.value }))
          }
          style={{ ...style, flex: 1 }}
          placeholder="0.00"
        />
        <input
          type="text"
          value={parsed.currency_code || ""}
          onChange={(e) =>
            onChange(
              JSON.stringify({ ...parsed, currency_code: e.target.value }),
            )
          }
          style={{ ...style, width: 80 }}
          placeholder="USD"
          maxLength={3}
        />
      </div>
    );
  }
  if (type?.startsWith("list.") || type?.endsWith("_reference")) {
    return (
      <textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        onFocus={onFocus}
        onBlur={onBlur}
        style={{
          ...style,
          minHeight: 70,
          resize: "vertical",
          fontFamily: "monospace",
          fontSize: 12,
        }}
        placeholder={
          type?.startsWith("list.") ? '["value1", "value2"]' : "gid://shopify/…"
        }
      />
    );
  }
  return (
    <input
      type="text"
      value={value}
      onChange={(e) => onChange(e.target.value)}
      style={style}
      onFocus={onFocus}
      onBlur={onBlur}
      autoFocus={autoFocus}
      placeholder={placeholder}
    />
  );
}

// ── Button ─────────────────────────────────────────────────────────────────────
function Btn({ children, onClick, disabled, variant = "secondary", style: s }) {
  const [hov, setHov] = useState(false);
  const map = {
    primary: {
      background: hov ? "var(--accent)" : C.accent,
      color: C.text,
      border: "none",
    },
    secondary: {
      background: hov ? C.border : C.card,
      color: C.muted,
      border: `1px solid ${C.border}`,
    },
    danger: {
      background: hov ? "var(--danger-light)" : "transparent",
      color: C.danger,
      border: `1px solid ${hov ? C.danger : C.border}`,
    },
    ghost: { background: "transparent", color: C.muted, border: "none" },
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
        gap: 5,
        padding: "7px 14px",
        borderRadius: 7,
        fontSize: 13,
        fontWeight: 600,
        fontFamily: "inherit",
        cursor: disabled ? "not-allowed" : "pointer",
        transition: "all .15s",
        opacity: disabled ? 0.5 : 1,
        ...map[variant],
        ...s,
      }}
    >
      {children}
    </button>
  );
}

// ── Definition row — shown when definition exists but no value yet ─────────────
function DefinitionRow({
  def,
  resource,
  resourceId,
  toast,
  onSaved,
  batchMode,
  onBatchAdd,
}) {
  const [value, setValue] = useState("");
  const [saving, setSaving] = useState(false);
  const [editing, setEditing] = useState(false);
  const type = def.type?.name || "single_line_text_field";
  const tc = getTypeColor(type);

  const handleSave = async () => {
    // Validate that value is not empty
    const formattedValue = formatForSave(value, type);
    if (!formattedValue || formattedValue === '""' || formattedValue === "[]") {
      toast?.("Value cannot be empty", "error");
      return;
    }

    if (batchMode) {
      // Batch mode: collect the change instead of saving immediately
      onBatchAdd?.({
        namespace: def.namespace,
        key: def.key,
        type,
        value: formattedValue,
        isNew: true,
      });
      setEditing(false);
      setValue("");
      return;
    }

    // Immediate mode: save directly
    setSaving(true);
    try {
      const data = await api.post(`/metafields/${resource}/${resourceId}`, {
        namespace: def.namespace,
        key: def.key,
        type,
        value: formatForSave(value, type),
      });
      toast?.("Saved!", "success");
      setEditing(false);
      setValue("");
      onSaved();
    } catch (e) {
      console.error("Save error:", e);
      toast?.(e.message, "error");
    }
    setSaving(false);
  };

  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 16,
        padding: "14px 0",
        borderBottom: `1px solid ${C.border}`,
      }}
    >
      {/* Label */}
      <div style={{ width: 200, flexShrink: 0, paddingTop: 2 }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: C.text }}>
          {def.name || def.key}
        </div>
        <code
          style={{
            fontSize: 10,
            color: C.disabled,
            display: "block",
            marginTop: 2,
          }}
        >
          {def.namespace}.{def.key}
        </code>
        <span
          style={{
            background: tc.bg,
            color: tc.c,
            fontSize: 9,
            padding: "2px 5px",
            borderRadius: 3,
            fontWeight: 700,
            display: "inline-block",
            marginTop: 4,
          }}
        >
          {getTypeLabel(type)}
        </span>
      </div>

      {/* Input area */}
      <div style={{ flex: 1 }}>
        {editing ? (
          <div>
            <ValueInput
              type={type}
              value={value}
              onChange={setValue}
              placeholder={def.description || "Enter value…"}
              options={extractOptionsFromValidations(def.validations)}
            />
            <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
              <Btn
                onClick={handleSave}
                disabled={saving}
                variant="primary"
                style={{ fontSize: 12, padding: "5px 12px" }}
              >
                {saving ? <Spin size={11} /> : batchMode ? "Add" : "Save"}
              </Btn>
            </div>
          </div>
        ) : (
          <div
            onClick={() => setEditing(true)}
            style={{
              padding: "8px 12px",
              background: C.card,
              border: `1px dashed ${C.border}`,
              borderRadius: 7,
              fontSize: 12,
              color: C.disabled,
              cursor: "text",
              transition: "border-color .15s",
            }}
            onMouseEnter={(e) => (e.currentTarget.style.borderColor = C.accent)}
            onMouseLeave={(e) => (e.currentTarget.style.borderColor = C.border)}
          >
            {def.description || "Click to add value…"}
          </div>
        )}
      </div>
    </div>
  );
}

// ── Existing metafield row (already has a value) ───────────────────────────────
function MetafieldRow({
  mf,
  definition,
  defName,
  toast,
  onSaved,
  onDeleted,
  batchMode,
  onBatchUpdate,
}) {
  const [editing, setEditing] = useState(false);
  const [editVal, setEditVal] = useState("");
  const [saving, setSaving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const tc = getTypeColor(mf.type);

  const startEdit = () => {
    setEditing(true);
    setEditVal(
      typeof mf.value === "object"
        ? JSON.stringify(mf.value)
        : String(mf.value ?? ""),
    );
  };

  // Auto-track changes in batch mode
  useEffect(() => {
    if (
      editing &&
      batchMode &&
      editVal !==
        (typeof mf.value === "object"
          ? JSON.stringify(mf.value)
          : String(mf.value ?? ""))
    ) {
      // Value has changed, auto-track it
      onBatchUpdate?.({
        id: mf.id,
        value: formatForSave(editVal, mf.type),
      });
    }
  }, [editVal, editing, batchMode, mf.id, mf.value, mf.type, onBatchUpdate]);

  const handleSave = async () => {
    // Immediate mode only
    setSaving(true);
    try {
      await api.put(`/metafields/${mf.id}`, {
        value: formatForSave(editVal, mf.type),
      });
      toast?.("Updated!", "success");
      setEditing(false);
      onSaved();
    } catch (e) {
      console.error("Update error:", e);
      toast?.(e.message || "Failed to update metafield", "error");
    }
    setSaving(false);
  };

  const handleDelete = async () => {
    if (!window.confirm("Delete this metafield?")) return;
    setDeleting(true);
    try {
      await api.delete(`/metafields/${mf.id}`);
      toast?.("Deleted", "success");
      onDeleted(mf.id);
    } catch (e) {
      toast?.(e.message, "error");
    }
    setDeleting(false);
  };

  const displayed = formatForDisplay(mf.value);

  return (
    <div
      style={{
        display: "flex",
        alignItems: "flex-start",
        gap: 16,
        padding: "14px 0",
        borderBottom: `1px solid ${C.border}`,
      }}
    >
      {/* Label */}
      <div style={{ width: 200, flexShrink: 0, paddingTop: 2 }}>
        <div style={{ fontSize: 13, fontWeight: 500, color: C.text }}>
          {defName || mf.key}
        </div>
        <code
          style={{
            fontSize: 10,
            color: C.disabled,
            display: "block",
            marginTop: 2,
          }}
        >
          {mf.namespace}.{mf.key}
        </code>
        <span
          style={{
            background: tc.bg,
            color: tc.c,
            fontSize: 9,
            padding: "2px 5px",
            borderRadius: 3,
            fontWeight: 700,
            display: "inline-block",
            marginTop: 4,
          }}
        >
          {getTypeLabel(mf.type)}
        </span>
      </div>

      {/* Value / edit */}
      <div style={{ flex: 1 }}>
        {editing ? (
          <div>
            <ValueInput
              type={mf.type}
              value={editVal}
              onChange={setEditVal}
              options={
                definition
                  ? extractOptionsFromValidations(definition.validations)
                  : null
              }
              onInputBlur={() => (batchMode ? setEditing(false) : null)}
              autoFocus
            />
            {!batchMode && (
              <div style={{ display: "flex", gap: 8, marginTop: 8 }}>
                <Btn
                  onClick={handleSave}
                  disabled={saving}
                  variant="primary"
                  style={{ fontSize: 12, padding: "5px 12px" }}
                >
                  {saving ? <Spin size={11} /> : "Save"}
                </Btn>
              </div>
            )}
          </div>
        ) : (
          <div
            onClick={startEdit}
            style={{
              padding: "8px 12px",
              background: C.card,
              border: `1px solid ${C.border}`,
              borderRadius: 7,
              fontSize: 12,
              color: displayed ? C.text : C.disabled,
              cursor: "text",
              transition: "border-color .15s",
              fontFamily: ["json", "rich_text_field"].includes(mf.type)
                ? "monospace"
                : "inherit",
              wordBreak: "break-all",
            }}
            onMouseEnter={(e) => (e.currentTarget.style.borderColor = C.accent)}
            onMouseLeave={(e) => (e.currentTarget.style.borderColor = C.border)}
          >
            {displayed || "—"}
          </div>
        )}
      </div>

      {/* Actions */}
      {!editing && (
        <div style={{ display: "flex", gap: 6, flexShrink: 0, paddingTop: 2 }}>
          <Btn
            onClick={startEdit}
            variant="ghost"
            style={{ fontSize: 11, padding: "4px 8px" }}
          >
            Edit
          </Btn>
          <Btn
            onClick={handleDelete}
            disabled={deleting}
            variant="danger"
            style={{ fontSize: 11, padding: "4px 8px" }}
          >
            {deleting ? <Spin size={10} /> : "✕"}
          </Btn>
        </div>
      )}
    </div>
  );
}

// ── Add custom metafield form ──────────────────────────────────────────────────
function AddCustomForm({ resource, resourceId, toast, onSaved, onClose }) {
  const [form, setForm] = useState({
    namespace: "custom",
    key: "",
    type: "single_line_text_field",
    value: "",
  });
  const [saving, setSaving] = useState(false);

  const handleCreate = async () => {
    if (!form.key.trim()) {
      toast?.("Key is required", "error");
      return;
    }
    setSaving(true);
    try {
      await api.post(`/metafields/${resource}/${resourceId}`, {
        namespace: form.namespace || "custom",
        key: form.key,
        type: form.type,
        value: formatForSave(form.value, form.type),
      });
      toast?.("Metafield created!", "success");
      onSaved();
      onClose();
    } catch (e) {
      toast?.(e.message, "error");
    }
    setSaving(false);
  };

  return (
    <div
      style={{
        background: C.card,
        border: `1px solid ${C.accent}`,
        borderRadius: 8,
        padding: 16,
        marginBottom: 16,
      }}
    >
      <div
        style={{
          fontSize: 13,
          fontWeight: 600,
          color: C.text,
          marginBottom: 14,
        }}
      >
        Add custom metafield
      </div>
      <div
        style={{
          display: "grid",
          gridTemplateColumns: "1fr 1fr",
          gap: 10,
          marginBottom: 10,
        }}
      >
        <div>
          <label
            style={{
              fontSize: 11,
              color: C.muted,
              display: "block",
              marginBottom: 4,
            }}
          >
            Namespace
          </label>
          <input
            value={form.namespace}
            onChange={(e) =>
              setForm((p) => ({ ...p, namespace: e.target.value }))
            }
            style={baseInput}
            placeholder="custom"
          />
        </div>
        <div>
          <label
            style={{
              fontSize: 11,
              color: C.muted,
              display: "block",
              marginBottom: 4,
            }}
          >
            Key *
          </label>
          <input
            value={form.key}
            onChange={(e) => setForm((p) => ({ ...p, key: e.target.value }))}
            style={baseInput}
            placeholder="my_field"
          />
        </div>
      </div>
      <div style={{ marginBottom: 10 }}>
        <label
          style={{
            fontSize: 11,
            color: C.muted,
            display: "block",
            marginBottom: 4,
          }}
        >
          Type
        </label>
        <div style={{ position: "relative" }}>
          <select
            value={form.type}
            onChange={(e) =>
              setForm((p) => ({ ...p, type: e.target.value, value: "" }))
            }
            style={{
              ...baseInput,
              appearance: "none",
              cursor: "pointer",
              paddingRight: 32,
            }}
          >
            {METAFIELD_TYPE_GROUPS.map((group) => (
              <optgroup key={group.group} label={group.group}>
                {group.types.map((t) => (
                  <option key={t.value} value={t.value}>
                    {t.label}
                  </option>
                ))}
              </optgroup>
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
      </div>
      <div style={{ marginBottom: 14 }}>
        <label
          style={{
            fontSize: 11,
            color: C.muted,
            display: "block",
            marginBottom: 4,
          }}
        >
          Value
        </label>
        <ValueInput
          type={form.type}
          value={form.value}
          onChange={(v) => setForm((p) => ({ ...p, value: v }))}
        />
      </div>
      <div style={{ display: "flex", gap: 8 }}>
        <Btn onClick={handleCreate} disabled={saving} variant="primary">
          {saving ? <Spin size={12} /> : "Save metafield"}
        </Btn>
      </div>
    </div>
  );
}

// ── Main MetafieldsPanel ───────────────────────────────────────────────────────
export default function MetafieldsPanel({
  resource,
  resourceId,
  resourceName,
  toast,
  batchMode = true,
}) {
  const [metafields, setMetafields] = useState([]);
  const [definitions, setDefinitions] = useState([]);
  const [loading, setLoading] = useState(true);
  const [showAdd, setShowAdd] = useState(false);
  const [pendingChanges, setPendingChanges] = useState({
    new: [],
    updates: [],
  });
  const [saving, setSaving] = useState(false);

  // ── Fetch both metafields and definitions in parallel ──────────────────────
  const fetchAll = useCallback(async () => {
    if (!resourceId) return;
    setLoading(true);
    try {
      const [mfData, defData] = await Promise.all([
        api.get(`/metafields/${resource}/${resourceId}`),
        api.get(`/metafields/definitions/${resource}`),
      ]);
      setMetafields(mfData.metafields || []);
      setDefinitions(defData.definitions || []);
    } catch (e) {
      console.error("Failed to load metafields:", e);
      toast?.("Failed to load metafields", "error");
    }
    setLoading(false);
  }, [resource, resourceId]);

  // ── Batch mode handlers (memoized to prevent excessive re-renders) ─────────
  const handleBatchAdd = useCallback((newMetafield) => {
    setPendingChanges((p) => ({
      ...p,
      new: [...p.new, newMetafield],
    }));
  }, []);

  const handleBatchUpdate = useCallback((update) => {
    setPendingChanges((p) => {
      // Check if we already have an update for this metafield ID
      const existingIndex = p.updates.findIndex((u) => u.id === update.id);
      if (existingIndex >= 0) {
        // Replace the existing update with the new value
        const newUpdates = [...p.updates];
        newUpdates[existingIndex] = update;
        return { ...p, updates: newUpdates };
      } else {
        // Add new update
        return { ...p, updates: [...p.updates, update] };
      }
    });
  }, []);

  const handleSaveAll = async () => {
    if (
      pendingChanges.new.length === 0 &&
      pendingChanges.updates.length === 0
    ) {
      toast?.("No changes to save", "warning");
      return;
    }

    setSaving(true);
    try {
      // Save new metafields
      for (const mf of pendingChanges.new) {
        await api.post(`/metafields/${resource}/${resourceId}`, {
          namespace: mf.namespace,
          key: mf.key,
          type: mf.type,
          value: mf.value,
        });
      }

      // Update existing metafields
      for (const update of pendingChanges.updates) {
        await api.put(`/metafields/${update.id}`, {
          value: update.value,
        });
      }

      toast?.("All changes saved!", "success");
      setPendingChanges({ new: [], updates: [] });
      await fetchAll();
    } catch (e) {
      console.error("Batch save error:", e);
      toast?.(e.message || "Failed to save changes", "error");
    }
    setSaving(false);
  };

  useEffect(() => {
    fetchAll();
  }, [fetchAll]);

  // ── Build merged view (same logic as Shopify admin) ────────────────────────
  // For each definition → find matching metafield value if it exists
  const defRows = definitions.map((def) => ({
    def,
    existing:
      metafields.find(
        (mf) => mf.namespace === def.namespace && mf.key === def.key,
      ) || null,
  }));

  // Metafields that don't match any definition = custom/ad-hoc ones
  const definitionKeys = new Set(
    definitions.map((d) => `${d.namespace}.${d.key}`),
  );
  const customMetafields = metafields.filter(
    (mf) => !definitionKeys.has(`${mf.namespace}.${mf.key}`),
  );

  const handleDeleted = (id) =>
    setMetafields((prev) => prev.filter((m) => m.id !== id));

  const hasAnything = defRows.length > 0 || customMetafields.length > 0;

  // Auto-expand add form if no metafields exist
  const shouldAutoExpandAdd = !hasAnything && !showAdd;

  if (loading) {
    return (
      <div
        style={{
          display: "flex",
          justifyContent: "center",
          alignItems: "center",
          padding: 40,
        }}
      >
        <Spin size={22} />
      </div>
    );
  }

  return (
    <div style={{ fontFamily: "inherit" }}>
      {/* Header */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          marginBottom: 4,
          gap: 12,
          flexWrap: "wrap",
        }}
      >
        <div>
          <div style={{ fontSize: 15, fontWeight: 700, color: C.text }}>
            Metafields
            <span
              style={{
                fontSize: 12,
                fontWeight: 400,
                color: C.muted,
                marginLeft: 8,
              }}
            >
              {metafields.length} set
              {defRows.length > 0 ? ` / ${defRows.length} available` : ""}
            </span>
          </div>
          {resourceName && (
            <div style={{ fontSize: 11, color: C.muted, marginTop: 2 }}>
              {resourceName}
            </div>
          )}
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          {batchMode &&
            (pendingChanges.new.length > 0 ||
              pendingChanges.updates.length > 0) && (
              <Btn
                onClick={handleSaveAll}
                disabled={saving}
                variant="primary"
                style={{ fontSize: 12 }}
              >
                {saving ? (
                  <Spin size={11} />
                ) : (
                  `Save All (${pendingChanges.new.length + pendingChanges.updates.length})`
                )}
              </Btn>
            )}
          {!shouldAutoExpandAdd && (
            <Btn
              onClick={() => setShowAdd((p) => !p)}
              variant="primary"
              style={{ fontSize: 12 }}
            >
              {showAdd ? "Cancel" : "+ Add metafield"}
            </Btn>
          )}
        </div>
      </div>

      {/* Add custom form */}
      {(showAdd || shouldAutoExpandAdd) && (
        <AddCustomForm
          resource={resource}
          resourceId={resourceId}
          toast={toast}
          onSaved={fetchAll}
          onClose={() => setShowAdd(false)}
        />
      )}

      {/* Empty state with helpful info — only show if NO definitions exist */}
      {defRows.length === 0 && !hasAnything && !shouldAutoExpandAdd && (
        <div style={{ textAlign: "center", padding: "36px 0", color: C.muted }}>
          <div style={{ fontSize: 36, marginBottom: 10 }}>🏷️</div>
          <div style={{ fontSize: 14, fontWeight: 600, marginBottom: 4 }}>
            No metafields yet
          </div>
          <div style={{ fontSize: 12, marginBottom: 14 }}>
            Add custom data to extend this {resource.slice(0, -1)}
          </div>
          <button
            onClick={fetchAll}
            style={{
              fontSize: 12,
              padding: "6px 12px",
              background: C.accent,
              color: "#fff",
              border: "none",
              borderRadius: 6,
              cursor: "pointer",
              fontWeight: 600,
            }}
          >
            🔄 Refresh metafields from Shopify
          </button>
        </div>
      )}

      {/* Definition-based rows — shown even when empty, just like Shopify */}
      {defRows.length > 0 && (
        <div>
          <div
            style={{
              fontSize: 11,
              fontWeight: 700,
              color: C.muted,
              textTransform: "uppercase",
              letterSpacing: ".06em",
              padding: "14px 0 4px",
            }}
          >
            Available metafields ({metafields.length} set,{" "}
            {defRows.filter((r) => !r.existing).length} empty)
          </div>
          {defRows.map(({ def, existing }) =>
            existing ? (
              <MetafieldRow
                key={existing.id}
                mf={existing}
                definition={def}
                defName={def.name}
                toast={toast}
                onSaved={fetchAll}
                onDeleted={handleDeleted}
                batchMode={batchMode}
                onBatchUpdate={handleBatchUpdate}
              />
            ) : (
              <DefinitionRow
                key={`def-${def.id}`}
                def={def}
                resource={resource}
                resourceId={resourceId}
                toast={toast}
                onSaved={fetchAll}
                batchMode={batchMode}
                onBatchAdd={handleBatchAdd}
              />
            ),
          )}
        </div>
      )}

      {/* Custom / ad-hoc metafields with no matching definition */}
      {customMetafields.length > 0 && (
        <div>
          {defRows.length > 0 && (
            <div
              style={{
                fontSize: 11,
                fontWeight: 700,
                color: C.muted,
                textTransform: "uppercase",
                letterSpacing: ".06em",
                padding: "14px 0 4px",
              }}
            >
              Custom metafields
            </div>
          )}
          {customMetafields.map((mf) => (
            <MetafieldRow
              key={mf.id}
              mf={mf}
              defName={null}
              toast={toast}
              onSaved={fetchAll}
              onDeleted={handleDeleted}
              batchMode={batchMode}
              onBatchUpdate={handleBatchUpdate}
            />
          ))}
        </div>
      )}
    </div>
  );
}
