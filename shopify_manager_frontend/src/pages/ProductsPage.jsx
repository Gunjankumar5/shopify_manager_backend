import { useState, useEffect, useCallback, useMemo } from "react";
import { api } from "../api/api";
import { Ico, Spin } from "../components/Icons";
import { Badge } from "../components/UI";
import { PriceModal } from "../components/Modals";
import ProductCard from "../components/ProductCard";
import AddProductPage from "./Addproductpage";

const ProductsPage = ({ toast, activeStore }) => {
  const CLIENT_PAGE_SIZE = 50;
  const BACKEND_PAGE_SIZE = 250;
  const [products, setProducts] = useState([]);
  const [loading, setLoading] = useState(true);
  const [view, setView] = useState("grid");
  const [search, setSearch] = useState("");
  const [searchQuery, setSearchQuery] = useState("");
  const [statusF, setStatusF] = useState("all");
  const [sortBy, setSortBy] = useState("newest");
  const [sel, setSel] = useState(new Set());
  const [priceM, setPriceM] = useState(false);
  const [deleting, setDeleting] = useState(null);
  const [currentPage, setCurrentPage] = useState(1);
  const [viewportW, setViewportW] = useState(() => window.innerWidth);
  const [viewportH, setViewportH] = useState(() => window.innerHeight);

  // null = list view, true = new product form, object = edit product form
  const [formMode, setFormMode] = useState(null);

  // Helper to save formMode to localStorage (store only ID or "new")
  const saveFormMode = useCallback((mode) => {
    try {
      if (mode === null) {
        localStorage.removeItem("productFormMode");
      } else if (mode === true) {
        localStorage.setItem("productFormMode", JSON.stringify("new"));
      } else if (mode?.id) {
        localStorage.setItem("productFormMode", JSON.stringify(mode.id));
      }
    } catch {
      // Silently fail if localStorage is unavailable
    }
  }, []);

  // Initialize and restore formMode from localStorage
  useEffect(() => {
    try {
      const saved = localStorage.getItem("productFormMode");
      if (!saved) return;

      const parsed = JSON.parse(saved);
      if (parsed === "new") {
        setFormMode(true);
      } else if (typeof parsed === "string" && products.length > 0) {
        const product = products.find((p) => p.id === parsed);
        if (product) {
          setFormMode(product);
        } else if (!loading) {
          localStorage.removeItem("productFormMode");
        }
      }
    } catch {
      // Silently fail if there's an error
    }
  }, [products, loading]);

  // Save formMode whenever it changes
  useEffect(() => {
    saveFormMode(formMode);
  }, [formMode, saveFormMode]);

  const loadInitialProducts = useCallback(
    async ({ force = false } = {}) => {
      if (!activeStore?.shop_key) {
        setProducts([]);
        setCurrentPage(1);
        setLoading(false);
        return;
      }

      setLoading(true);
      setProducts([]);
      setCurrentPage(1);

      try {
        const all = [];
        let cursor = null;
        let hasNextPage = true;
        let pagesFetched = 0;
        const maxPages = 500;

        while (hasNextPage && pagesFetched < maxPages) {
          const params = new URLSearchParams({
            limit: String(BACKEND_PAGE_SIZE),
          });
          if (statusF && statusF !== "all") params.set("status", statusF);
          if (searchQuery) params.set("search", searchQuery);
          if (cursor) params.set("page_info", cursor);

          const d = await api.get(`/products?${params.toString()}`, {
            force: force || Boolean(cursor),
          });

          const batch = d.products || [];
          all.push(...batch);

          cursor = d.next_page_info || null;
          hasNextPage = Boolean(d.has_next_page && cursor);
          pagesFetched += 1;
        }

        setProducts(all);
      } catch (error) {
        toast(`Failed to load products: ${error.message}`, "error");
      } finally {
        setLoading(false);
      }
    },
    [statusF, searchQuery, toast, activeStore?.shop_key],
  );

  const loadAllProductsForBulkOps = useCallback(
    async ({ force = false } = {}) => {
      if (!activeStore?.shop_key) {
        setProducts([]);
        setCurrentPage(1);
        setLoading(false);
        return;
      }

      setLoading(true);

      try {
        const all = [];
        let cursor = null;
        let hasNextPage = true;
        let pagesFetched = 0;
        const maxPages = 500;

        while (hasNextPage && pagesFetched < maxPages) {
          const params = new URLSearchParams({
            limit: String(BACKEND_PAGE_SIZE),
          });
          if (statusF && statusF !== "all") params.set("status", statusF);
          if (searchQuery) params.set("search", searchQuery);
          if (cursor) params.set("page_info", cursor);

          const d = await api.get(`/products?${params.toString()}`, {
            force: force || Boolean(cursor),
          });

          const batch = d.products || [];
          all.push(...batch);

          cursor = d.next_page_info || null;
          hasNextPage = Boolean(d.has_next_page && cursor);
          pagesFetched += 1;
        }

        setProducts(all);
      } catch (error) {
        toast(`Failed to load products: ${error.message}`, "error");
      } finally {
        setLoading(false);
      }
    },
    [statusF, searchQuery, toast, activeStore?.shop_key],
  );

  useEffect(() => {
    const onResize = () => {
      setViewportW(window.innerWidth);
      setViewportH(window.innerHeight);
    };
    window.addEventListener("resize", onResize);
    return () => window.removeEventListener("resize", onResize);
  }, []);

  useEffect(() => {
    const id = window.setTimeout(() => {
      setSearchQuery(search.trim());
    }, 300);
    return () => window.clearTimeout(id);
  }, [search]);

  // ── FIX: removed loadAllProducts from deps — it was causing infinite loop ───
  useEffect(() => {
    setSel(new Set());
    loadInitialProducts();
  }, [statusF, searchQuery, activeStore?.shop_key, loadInitialProducts]);

  const filtered = useMemo(() => {
    let sorted = [...products];

    // Apply sorting
    switch (sortBy) {
      case "newest":
        sorted.sort((a, b) => {
          const dateA = new Date(a.created_at || 0).getTime();
          const dateB = new Date(b.created_at || 0).getTime();
          return dateB - dateA;
        });
        break;
      case "updated":
        sorted.sort((a, b) => {
          const dateA = new Date(a.updated_at || a.created_at || 0).getTime();
          const dateB = new Date(b.updated_at || b.created_at || 0).getTime();
          return dateB - dateA;
        });
        break;
      case "oldest":
        sorted.sort((a, b) => {
          const dateA = new Date(a.created_at || 0).getTime();
          const dateB = new Date(b.created_at || 0).getTime();
          return dateA - dateB;
        });
        break;
      case "name_asc":
        sorted.sort((a, b) => (a.title || "").localeCompare(b.title || ""));
        break;
      case "name_desc":
        sorted.sort((a, b) => (b.title || "").localeCompare(a.title || ""));
        break;
      default:
        break;
    }

    return sorted;
  }, [products, sortBy]);

  const productCount = filtered.length;
  const totalPages = Math.max(1, Math.ceil(productCount / CLIENT_PAGE_SIZE));
  const safeCurrentPage = Math.min(currentPage, totalPages);

  useEffect(() => {
    if (currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  const pageStart = (safeCurrentPage - 1) * CLIENT_PAGE_SIZE;
  const pagedProducts = useMemo(
    () => filtered.slice(pageStart, pageStart + CLIENT_PAGE_SIZE),
    [filtered, pageStart],
  );

  const gridColumns = useMemo(() => {
    if (viewportW < 640) return 2;
    if (viewportW < 900) return 3;
    if (viewportW < 1200) return 4;
    return 5;
  }, [viewportW]);

  const gridGap = 12;
  const gridContainerWidth = Math.max(320, viewportW - 360);
  const gridItemWidth = Math.max(
    180,
    Math.floor(
      (gridContainerWidth - gridGap * (gridColumns + 1)) / gridColumns,
    ),
  );
  const gridItemHeight = 395;
  const gridRows = Math.ceil(pagedProducts.length / gridColumns);
  const virtualHeight = Math.max(320, Math.min(760, viewportH - 360));

  const toggleSel = (id) =>
    setSel((prev) => {
      const next = new Set(prev);
      next.has(id) ? next.delete(id) : next.add(id);
      return next;
    });

  const selAll = () =>
    setSel(
      sel.size === filtered.length
        ? new Set()
        : new Set(filtered.map((p) => p.id)),
    );

  const handleDel = async (id) => {
    if (!window.confirm("Delete this product?")) return;
    setDeleting(id);
    try {
      await api.delete(`/products/${id}`);
      toast("Deleted");
      loadAllProductsForBulkOps({ force: true });
    } catch {
      toast("Failed", "error");
    }
    setDeleting(null);
  };

  const handleRemoveDuplicates = async () => {
    try {
      const preview = await api.get("/products/find-duplicates");
      if (preview.duplicates_found === 0) {
        toast("No duplicate products found! ✅", "success");
        return;
      }
      const list = preview.duplicates.map((d) => `• ${d.title}`).join("\n");
      if (
        !window.confirm(
          `Found ${preview.duplicates_found} duplicate(s):\n\n${list}\n\nDelete them? (keeps the first one)`,
        )
      )
        return;
      const result = await api.post("/products/remove-duplicates", {});
      toast(
        `Deleted ${result.deleted} duplicate(s)!${result.failed ? ` (${result.failed} failed)` : ""}`,
        result.deleted > 0 ? "success" : "error",
      );
      loadAllProductsForBulkOps({ force: true });
    } catch {
      toast("Failed to remove duplicates", "error");
    }
  };

  const handleBulkDel = async () => {
    if (!sel.size || !window.confirm(`Delete ${sel.size} products?`)) return;
    const results = await Promise.allSettled(
      Array.from(sel).map((id) => api.delete(`/products/${id}`)),
    );
    const ok = results.filter((result) => result.status === "fulfilled").length;
    const fail = results.length - ok;
    toast(`Deleted ${ok}${fail ? `, ${fail} failed` : ""}`);
    setSel(new Set());
    loadAllProductsForBulkOps({ force: true });
  };

  const handlePriceAdj = async ({ mode, value, dir }) => {
    const prods = products.filter((p) => sel.has(p.id));
    const priceUpdateRequests = prods
      .map((p) => {
        const v = p.variants?.[0];
        if (!v?.price) return null;
        const cur = parseFloat(v.price);
        let np =
          mode === "percent"
            ? dir === "increase"
              ? cur * (1 + value / 100)
              : cur * (1 - value / 100)
            : dir === "increase"
              ? cur + value
              : cur - value;
        np = Math.max(0, np).toFixed(2);
        return api.put(`/products/${p.id}`, {
          title: p.title,
          variants: [{ ...v, price: np }],
        });
      })
      .filter(Boolean);

    const results = await Promise.allSettled(priceUpdateRequests);
    const ok = results.filter((result) => result.status === "fulfilled").length;
    const fail = results.length - ok;
    toast(`Updated ${ok} prices${fail ? `, ${fail} failed` : ""}`);
    setPriceM(false);
    setSel(new Set());
    loadAllProductsForBulkOps({ force: true });
  };

  const stats = useMemo(
    () => ({
      total: products.length,
      active: products.filter((p) => p.status === "active").length,
      draft: products.filter((p) => p.status === "draft").length,
    }),
    [products],
  );

  // Render form or list view
  if (formMode !== null) {
    return (
      <AddProductPage
        toast={toast}
        editProduct={formMode === true ? null : formMode}
        onBack={() => {
          setFormMode(null);
          localStorage.removeItem("productFormMode");
          loadInitialProducts({ force: true });
        }}
      />
    );
  }

  return (
    <div
      className="fade-up container max-w-7xl mx-auto px-4 pt-4"
      style={{ position: "relative", minHeight: "calc(100vh - 120px)" }}
    >
      {priceM && (
        <PriceModal
          count={sel.size}
          onApply={handlePriceAdj}
          onClose={() => setPriceM(false)}
        />
      )}

      {/* Header */}
      <div style={{ marginBottom: 21 }}>
        <h1
          className="font-display text-3xl font-bold flex items-center gap-3"
          style={{
            paddingTop: "12px",
            color: "var(--text-primary)",
          }}
        >
          <Ico n="products" size="lg" /> Products
        </h1>
        <p
          style={{
            color: "var(--text-muted)",
            fontSize: "9.75px",
            marginTop: "6px",
          }}
        >
          Manage your Shopify product catalog
        </p>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-6">
        {[
          {
            label: "Total",
            value: stats.total,
            color: "accent",
            icon: "products",
          },
          {
            label: "Active",
            value: stats.active,
            color: "success",
            icon: "check",
          },
          {
            label: "Drafts",
            value: stats.draft,
            color: "warning",
            icon: "edit",
          },
        ].map((s) => (
          <div
            key={s.label}
            className="card flex items-center gap-3 p-4 rounded-xl transition-all hover:shadow-md"
            style={{
              background: "var(--bg-card)",
              border: "1px solid var(--border-subtle)",
            }}
          >
            <div
              className="w-10 h-10 rounded-lg flex items-center justify-center flex-shrink-0"
              style={{
                backgroundColor: `var(--${s.color}-light)`,
                color: `var(--${s.color})`,
              }}
            >
              <Ico n={s.icon} size={18} color={`var(--${s.color})`} />
            </div>
            <div>
              <div
                style={{
                  fontFamily: "var(--font-display)",
                  fontSize: "15px",
                  fontWeight: "bold",
                  color: `var(--${s.color})`,
                  lineHeight: "1.2",
                }}
              >
                {loading ? "—" : s.value}
              </div>
              <div
                style={{
                  color: "var(--text-muted)",
                  fontSize: "9px",
                  marginTop: "1.5px",
                }}
              >
                {s.label}
              </div>
            </div>
          </div>
        ))}
      </div>

      {/* Toolbar */}
      <div
        className="flex flex-row flex-wrap items-center gap-3 p-4 rounded-xl mb-6"
        style={{
          background: "var(--bg-card)",
          border: "1px solid var(--border-strong)",
          backdropFilter: "blur(8px)",
        }}
      >
        {/* Search */}
        <div className="relative flex-1 min-w-[220px] group">
          <div
            className="absolute left-4 top-1/2 -translate-y-1/2 transition-colors pointer-events-none"
            style={{ color: "var(--text-muted)" }}
          >
            <Ico n="search" size={16} />
          </div>
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search products..."
            className="w-full pl-11 pr-4 py-2.5 text-sm rounded-lg transition-all outline-none placeholder:text-muted/60"
            style={{
              background: "var(--bg-input)",
              border: "1px solid var(--border-subtle)",
              color: "var(--text-primary)",
            }}
            onFocus={(e) => {
              e.currentTarget.style.borderColor = "var(--accent)";
              e.currentTarget.style.boxShadow =
                "0 0 0 3px rgba(99,102,241,0.1)";
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = "var(--border-subtle)";
              e.currentTarget.style.boxShadow = "none";
            }}
          />
        </div>

        {/* Status filter */}
        <div className="relative">
          <select
            value={statusF}
            onChange={(e) => setStatusF(e.target.value)}
            className="appearance-none cursor-pointer px-4 py-2.5 pr-10 text-sm rounded-lg transition-all outline-none font-medium"
            style={{
              background: "var(--bg-input)",
              border: "1px solid var(--border-subtle)",
              color: "var(--text-primary)",
            }}
            onFocus={(e) => {
              e.currentTarget.style.borderColor = "var(--accent)";
              e.currentTarget.style.boxShadow =
                "0 0 0 3px rgba(99,102,241,0.1)";
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = "var(--border-subtle)";
              e.currentTarget.style.boxShadow = "none";
            }}
          >
            {[
              { v: "all", l: "All Status" },
              { v: "active", l: "Active" },
              { v: "draft", l: "Draft" },
              { v: "archived", l: "Archived" },
            ].map((o) => (
              <option key={o.v} value={o.v}>
                {o.l}
              </option>
            ))}
          </select>
          <div
            className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none"
            style={{ color: "var(--text-muted)" }}
          >
            <Ico n="chevron-down" size={14} />
          </div>
        </div>

        {/* Sort dropdown */}
        <div className="relative">
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value)}
            className="appearance-none cursor-pointer px-4 py-2.5 pr-10 text-sm rounded-lg transition-all outline-none font-medium"
            style={{
              background: "var(--bg-input)",
              border: "1px solid var(--border-subtle)",
              color: "var(--text-primary)",
            }}
            onFocus={(e) => {
              e.currentTarget.style.borderColor = "var(--accent)";
              e.currentTarget.style.boxShadow =
                "0 0 0 3px rgba(99,102,241,0.1)";
            }}
            onBlur={(e) => {
              e.currentTarget.style.borderColor = "var(--border-subtle)";
              e.currentTarget.style.boxShadow = "none";
            }}
          >
            {[
              { v: "newest", l: "Newest First" },
              { v: "updated", l: "Recently Updated" },
              { v: "oldest", l: "Oldest First" },
              { v: "name_asc", l: "Name (A-Z)" },
              { v: "name_desc", l: "Name (Z-A)" },
            ].map((o) => (
              <option key={o.v} value={o.v}>
                {o.l}
              </option>
            ))}
          </select>
          <div
            className="absolute right-3 top-1/2 -translate-y-1/2 pointer-events-none"
            style={{ color: "var(--text-muted)" }}
          >
            <Ico n="chevron-down" size={14} />
          </div>
        </div>

        {/* View toggle */}
        <div
          className="flex p-1 rounded-lg"
          style={{
            background: "var(--bg-elevated)",
            border: "1px solid var(--border-subtle)",
          }}
        >
          {[
            { id: "grid", icon: "grid" },
            { id: "list", icon: "list" },
          ].map((v) => (
            <button
              key={v.id}
              onClick={() => setView(v.id)}
              className="flex items-center justify-center w-9 h-9 rounded-md transition-all font-medium"
              style={{
                background: view === v.id ? "var(--bg-card)" : "transparent",
                boxShadow: view === v.id ? "var(--shadow-sm)" : "none",
                color: view === v.id ? "var(--accent)" : "var(--text-muted)",
                cursor: "pointer",
              }}
              onMouseEnter={(e) => {
                if (view !== v.id)
                  e.currentTarget.style.color = "var(--text-primary)";
              }}
              onMouseLeave={(e) => {
                if (view !== v.id)
                  e.currentTarget.style.color = "var(--text-muted)";
              }}
              aria-label={`${v.id} view`}
            >
              <Ico n={v.icon} size={16} />
            </button>
          ))}
        </div>

        {/* Actions */}
        <div className="flex items-center gap-2 ml-auto">
          <button
            onClick={() => loadInitialProducts({ force: true })}
            disabled={loading}
            className="flex items-center gap-2 px-4 py-2.5 text-sm font-medium rounded-lg transition-all"
            style={{
              background: "var(--bg-elevated)",
              border: "1px solid var(--border-subtle)",
              color: "var(--text-secondary)",
              cursor: loading ? "not-allowed" : "pointer",
              opacity: loading ? 0.6 : 1,
            }}
            onMouseEnter={(e) => {
              if (!loading) {
                e.currentTarget.style.background = "var(--bg-card)";
                e.currentTarget.style.color = "var(--text-primary)";
              }
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "var(--bg-elevated)";
              e.currentTarget.style.color = "var(--text-secondary)";
            }}
          >
            {loading ? <Spin size={16} /> : <Ico n="sync" size={16} />}
            <span className="hidden sm:inline">Refresh</span>
          </button>

          <button
            onClick={() => setFormMode(true)}
            className="flex items-center gap-2 px-5 py-2.5 text-sm font-bold rounded-lg transition-all active:scale-95 sm:flex"
            style={{
              background: "linear-gradient(135deg, var(--accent), #a855f7)",
              color: "white",
              boxShadow: "0 4px 15px rgba(99,102,241,0.3)",
              cursor: "pointer",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.boxShadow =
                "0 6px 20px rgba(99,102,241,0.4)";
              e.currentTarget.style.transform = "translateY(-2px)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.boxShadow =
                "0 4px 15px rgba(99,102,241,0.3)";
              e.currentTarget.style.transform = "translateY(0)";
            }}
          >
            <Ico n="plus" size={16} />
            <span>Add Product</span>
          </button>

          {/* Mobile Add Button */}
          <button
            onClick={() => setFormMode(true)}
            className="sm:hidden flex items-center justify-center w-10 h-10 rounded-lg transition-all active:scale-95"
            style={{
              background: "linear-gradient(135deg, var(--accent), #a855f7)",
              color: "white",
              boxShadow: "0 4px 15px rgba(99,102,241,0.3)",
              cursor: "pointer",
            }}
          >
            <Ico n="plus" size={16} />
          </button>
        </div>
      </div>

      {/* Bulk Actions */}
      {sel.size > 0 && (
        <div
          className="p-4 mb-4 rounded-lg flex flex-wrap items-center gap-3 fade-up border"
          style={{
            background: "var(--accent-light)",
            borderColor: "var(--accent)/25",
          }}
        >
          <span
            className="text-sm font-bold"
            style={{ color: "var(--accent)" }}
          >
            {sel.size} selected
          </span>
          <div className="flex-1" />
          <button
            onClick={() => setPriceM(true)}
            className="btn btn-secondary btn-sm flex items-center gap-2 px-3 py-2 rounded-lg transition-all font-medium"
            style={{
              background: "rgba(100, 100, 255, 0.1)",
              color: "var(--accent)",
              border: "1px solid var(--accent)/25",
              cursor: "pointer",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(100, 100, 255, 0.15)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "rgba(100, 100, 255, 0.1)";
            }}
          >
            <Ico n="percent" size={14} color="var(--accent)" /> Adjust Prices
          </button>
          <button
            onClick={handleRemoveDuplicates}
            className="btn btn-secondary btn-sm flex items-center gap-2 px-3 py-2 rounded-lg transition-all font-medium"
            style={{
              background: "rgba(255, 193, 7, 0.1)",
              color: "var(--warning)",
              border: "1px solid var(--warning)/25",
              cursor: "pointer",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(255, 193, 7, 0.15)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "rgba(255, 193, 7, 0.1)";
            }}
          >
            <Ico n="percent" size={14} color="var(--warning)" /> Remove
            Duplicates
          </button>
          <button
            onClick={handleBulkDel}
            className="btn btn-danger btn-sm flex items-center gap-2 px-3 py-2 rounded-lg transition-all font-medium"
            style={{
              background: "rgba(239, 68, 68, 0.1)",
              color: "var(--danger)",
              border: "1px solid var(--danger)/25",
              cursor: "pointer",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(239, 68, 68, 0.15)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "rgba(239, 68, 68, 0.1)";
            }}
          >
            <Ico n="trash" size={14} /> Delete
          </button>
          <button
            onClick={() => setSel(new Set())}
            className="btn btn-secondary btn-sm flex items-center justify-center w-9 h-9 rounded-lg transition-all"
            style={{
              background: "rgba(0, 0, 0, 0.05)",
              color: "var(--text-secondary)",
              border: "1px solid var(--border-subtle)",
              cursor: "pointer",
              padding: 0,
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.background = "rgba(0, 0, 0, 0.08)";
              e.currentTarget.style.color = "var(--text-primary)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.background = "rgba(0, 0, 0, 0.05)";
              e.currentTarget.style.color = "var(--text-secondary)";
            }}
            aria-label="Clear selection"
          >
            <Ico n="x" size={14} />
          </button>
        </div>
      )}

      {/* Select All */}
      {filtered.length > 0 && (
        <div
          className="flex items-center gap-2 mb-2 p-2 rounded-lg hover:bg-secondary/30 transition-colors cursor-pointer"
          onClick={() => document.getElementById("selectAll").click()}
        >
          <input
            type="checkbox"
            className="chk"
            checked={sel.size === filtered.length && filtered.length > 0}
            onChange={selAll}
            id="selectAll"
          />
          <label
            htmlFor="selectAll"
            className="text-sm font-medium text-secondary cursor-pointer hover:text-primary transition-colors"
          >
            Select all {filtered.length} products
          </label>
        </div>
      )}

      {/* Pagination */}
      {!loading && (
        <div className="flex items-center justify-center gap-4 mb-3">
          <button
            type="button"
            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            disabled={safeCurrentPage <= 1}
            className="p-1 rounded-md transition-colors disabled:opacity-40 hover:bg-secondary/40"
          >
            <Ico n="chevron-left" size={16} color="var(--text-secondary)" />
          </button>

          <span className="text-xs text-secondary min-w-fit">
            Page{" "}
            <span className="font-semibold text-primary">
              {safeCurrentPage}
            </span>{" "}
            / {totalPages}
          </span>

          <button
            type="button"
            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
            disabled={safeCurrentPage >= totalPages}
            className="p-1 rounded-md transition-colors disabled:opacity-40 hover:bg-secondary/40"
          >
            <Ico n="chevron-right" size={16} color="var(--text-secondary)" />
          </button>
        </div>
      )}

      {/* Product Grid/List */}
      {loading ? (
        <div
          className={
            view === "grid"
              ? "grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-3"
              : "space-y-2"
          }
        >
          {[...Array(8)].map((_, i) => (
            <div
              key={i}
              className="skeleton"
              style={{ height: view === "grid" ? 280 : 60 }}
            />
          ))}
        </div>
      ) : view === "grid" ? (
        <div
          style={{
            width: "100%",
            display: "grid",
            gridTemplateColumns: `repeat(${gridColumns}, 1fr)`,
            gap: `${gridGap * 0.75}px`,
            padding: "9px",
          }}
        >
          {pagedProducts.length > 0 ? (
            pagedProducts.map((p) => (
              <ProductCard
                key={p.id}
                p={p}
                sel={sel.has(p.id)}
                onSel={toggleSel}
                onEdit={() => setFormMode(p)}
                onDel={handleDel}
              />
            ))
          ) : (
            <p className="text-muted text-center py-8 col-span-full">
              No products found
            </p>
          )}
        </div>
      ) : (
        <div className="card" style={{ width: "100%" }}>
          {pagedProducts.length > 0 ? (
            <div>
              {pagedProducts.map((p, index) => {
                const img = p.images?.[0]?.src;
                const price = p.variants?.[0]?.price;
                const inv = p.variants?.reduce(
                  (s, v) => s + (v.inventory_quantity || 0),
                  0,
                );
                return (
                  <div
                    key={p.id}
                    style={{
                      display: "grid",
                      gridTemplateColumns:
                        "34px minmax(220px,1.6fr) minmax(120px,1fr) 90px 70px 90px 92px",
                      alignItems: "center",
                      gap: 6,
                      padding: "6px 9px",
                      borderBottom: "1px solid var(--border-strong)",
                    }}
                  >
                    <input
                      type="checkbox"
                      className="chk"
                      checked={sel.has(p.id)}
                      onChange={() => toggleSel(p.id)}
                      aria-label={`Select ${p.title}`}
                    />
                    <div className="flex items-center gap-2 min-w-0">
                      <div className="w-8 h-8 rounded bg-secondary overflow-hidden flex items-center justify-center flex-shrink-0">
                        {img ? (
                          <img
                            src={img}
                            alt=""
                            className="w-full h-full object-cover"
                          />
                        ) : (
                          <Ico n="image" size={12} className="text-muted" />
                        )}
                      </div>
                      <div className="min-w-0">
                        <div className="font-medium text-primary text-xs truncate">
                          {p.title}
                        </div>
                        {p.product_type && (
                          <div className="text-[0.6rem] text-muted truncate">
                            {p.product_type}
                          </div>
                        )}
                      </div>
                    </div>
                    <div className="text-secondary text-xs truncate">
                      {p.vendor || "—"}
                    </div>
                    <div className="font-semibold text-accent text-xs">
                      {price ? `$${price}` : "—"}
                    </div>
                    <div
                      className={`text-xs ${inv > 0 ? "text-success" : "text-danger"}`}
                    >
                      {inv}
                    </div>
                    <div>
                      <Badge status={p.status} />
                    </div>
                    <div
                      className="flex gap-1 justify-end"
                      style={{ minWidth: 92 }}
                    >
                      <button
                        onClick={() => setFormMode(p)}
                        className="btn btn-secondary"
                        style={{ padding: "4px", fontSize: "0" }}
                        aria-label="Edit"
                      >
                        <Ico n="edit" size={12} color="var(--accent)" />
                      </button>
                      <button
                        onClick={() => handleDel(p.id)}
                        disabled={deleting === p.id}
                        className="btn btn-secondary"
                        style={{ padding: "4px", fontSize: "0" }}
                        aria-label="Delete"
                      >
                        {deleting === p.id ? (
                          <Spin size={12} />
                        ) : (
                          <Ico n="trash" size={12} color="var(--danger)" />
                        )}
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            <div className="p-6 text-center text-muted">No products</div>
          )}
        </div>
      )}

      {/* Pagination - Bottom */}
      {!loading && (
        <div className="flex items-center justify-center gap-4 mt-6 pb-4">
          <button
            type="button"
            onClick={() => setCurrentPage((p) => Math.max(1, p - 1))}
            disabled={safeCurrentPage <= 1}
            className="p-1 rounded-md transition-colors disabled:opacity-40 hover:bg-secondary/40"
          >
            <Ico n="chevron-left" size={16} color="var(--text-secondary)" />
          </button>

          <span className="text-xs text-secondary min-w-fit">
            Page{" "}
            <span className="font-semibold text-primary">
              {safeCurrentPage}
            </span>{" "}
            / {totalPages}
          </span>

          <button
            type="button"
            onClick={() => setCurrentPage((p) => Math.min(totalPages, p + 1))}
            disabled={safeCurrentPage >= totalPages}
            className="p-1 rounded-md transition-colors disabled:opacity-40 hover:bg-secondary/40"
          >
            <Ico n="chevron-right" size={16} color="var(--text-secondary)" />
          </button>
        </div>
      )}
    </div>
  );
};

export default ProductsPage;
