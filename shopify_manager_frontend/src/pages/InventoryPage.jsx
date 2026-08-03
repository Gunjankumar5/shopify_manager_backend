import React, { useState, useEffect } from "react";
import { api } from "../api/api";
import { Ico, Spin } from "../components/Icons";
import { PageLoadingOverlay } from "../components/UI";

export default function InventoryPage() {
  const [locations, setLocations] = useState([]);
  const [inventory, setInventory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [selectedLocation, setSelectedLocation] = useState(null);
  const [refreshing, setRefreshing] = useState(false);
  const [search, setSearch] = useState("");

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      setLoading(true);
      setError(null);

      const [locData, invData] = await Promise.all([
        api.get("/inventory/locations"),
        api.get("/inventory/levels"),
      ]);
      setLocations(locData.locations || []);
      setInventory(invData.inventory_levels || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const handleUpdateQuantity = async (itemId, locationId, quantity) => {
    if (isNaN(quantity)) return;
    const previousInventory = inventory;

    setInventory((prev) =>
      prev.map((item) =>
        item.inventory_item_id === itemId && item.location_id === locationId
          ? { ...item, available: quantity }
          : item,
      ),
    );

    try {
      await api.post(
        `/inventory/update?inventory_item_id=${itemId}&location_id=${locationId}&quantity=${quantity}`,
      );
    } catch (err) {
      setInventory(previousInventory);
      alert(`Error: ${err.message}`);
    }
  };

  const handleFetchFromShopify = async () => {
    try {
      setRefreshing(true);
      await fetchData();
    } finally {
      setRefreshing(false);
    }
  };

  if (loading) {
    return (
      <div
        className="min-h-screen bg-primary p-8"
        style={{ position: "relative" }}
      >
        <PageLoadingOverlay
          badge="LOADING INVENTORY"
          title="Loading inventory"
          subtitle="Fetching locations and stock levels from Shopify."
        />
      </div>
    );
  }

  // Filter by location, then by search
  const byLocation = selectedLocation
    ? inventory.filter((item) => item.location_id === selectedLocation)
    : inventory;

  const filteredInventory = search.trim()
    ? byLocation.filter(
        (item) =>
          (item.product_title || "")
            .toLowerCase()
            .includes(search.toLowerCase()) ||
          (item.variant_title || "")
            .toLowerCase()
            .includes(search.toLowerCase()),
      )
    : byLocation;

  return (
    <div
      className="min-h-screen bg-primary p-8"
      style={{ position: "relative" }}
    >
      {refreshing && (
        <PageLoadingOverlay
          badge="SYNCING INVENTORY"
          title="Refreshing inventory"
          subtitle="Pulling the latest inventory levels from Shopify."
        />
      )}
      <div className="container max-w-7xl mx-auto">
        {/* Header */}
        <div className="flex flex-col sm:flex-row justify-between items-start sm:items-center gap-4 mb-8">
          <div>
            <h1 className="font-display text-2xl font-bold text-primary flex items-center gap-2">
              <Ico n="inventory" size="lg" /> Inventory Management
            </h1>
            <p className="text-muted text-sm">
              Manage stock levels across your warehouse locations
            </p>
          </div>
          <button
            onClick={handleFetchFromShopify}
            disabled={loading || refreshing}
            className="btn btn-secondary"
          >
            {refreshing ? <Spin size="sm" /> : <Ico n="upload" size="sm" />}
            {refreshing ? "Syncing..." : "Sync from Shopify"}
          </button>
        </div>

        {/* Error message */}
        {error && (
          <div className="bg-danger-light text-danger p-4 rounded-lg mb-6 flex items-center gap-2 fade-up">
            <Ico n="alert-circle" size="sm" /> {error}
          </div>
        )}

        {/* Location Filter Cards */}
        <div className="card p-6 mb-8">
          <h2 className="font-display text-lg font-bold mb-4 flex items-center gap-2">
            <Ico n="location" size="md" /> Warehouse Locations
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
            <button
              onClick={() => setSelectedLocation(null)}
              className={`card card-hover p-4 text-left transition-all ${
                selectedLocation === null
                  ? "border-accent ring-1 ring-accent"
                  : "border-strong hover:border-accent"
              }`}
            >
              <div className="flex items-center gap-2 text-lg">
                <Ico n="grid" size="lg" />
                <span className="font-display font-bold">All Locations</span>
              </div>
              <p className="text-xs text-muted mt-2">
                View entire inventory across all warehouses
              </p>
            </button>

            {locations.map((location) => (
              <button
                key={location.id}
                onClick={() => setSelectedLocation(location.id)}
                className={`card card-hover p-4 text-left transition-all ${
                  selectedLocation === location.id
                    ? "border-accent ring-1 ring-accent"
                    : "border-strong hover:border-accent"
                }`}
              >
                <div className="flex items-center gap-2">
                  <Ico n="location" size="lg" />
                  <span className="font-display font-bold truncate">
                    {location.name}
                  </span>
                </div>
                <p className="text-xs text-muted mt-1 truncate">
                  {location.address1 || "Address not set"}
                </p>
                {location.city && (
                  <p className="text-xs text-muted">
                    {location.city}, {location.country_code}
                  </p>
                )}
              </button>
            ))}
          </div>
        </div>

        {/* Inventory Table */}
        <div className="card p-6">
          <div className="flex items-center justify-between mb-4 gap-4 flex-wrap">
            <h2 className="font-display text-lg font-bold flex items-center gap-2">
              <Ico n="inventory" size="md" /> Inventory Levels
              <span className="text-sm font-normal text-muted ml-2">
                ({filteredInventory.length} items)
              </span>
            </h2>
            {/* Search box */}
            <div className="relative">
              <div className="absolute left-3 top-1/2 -translate-y-1/2 text-muted pointer-events-none">
                <Ico n="search" size={14} />
              </div>
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search products..."
                className="pl-9 pr-4 py-2 text-sm rounded-lg outline-none"
                style={{
                  background: "var(--bg-input)",
                  border: "1px solid var(--border-strong)",
                  color: "var(--text-primary)",
                  width: 220,
                }}
              />
            </div>
          </div>

          {filteredInventory.length === 0 ? (
            <div className="text-center py-12">
              <Ico n="inventory" size="xl" className="text-muted mb-3" />
              <p className="text-secondary">
                No inventory items found for this location.
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto rounded-lg border border-strong">
              <table className="w-full">
                <thead>
                  <tr className="bg-secondary border-b border-strong">
                    <th className="px-4 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">
                      Product
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">
                      Variant
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">
                      Location
                    </th>
                    <th className="px-4 py-3 text-left text-xs font-semibold text-muted uppercase tracking-wider">
                      Quantity
                    </th>
                    <th className="px-4 py-3 text-center text-xs font-semibold text-muted uppercase tracking-wider">
                      Action
                    </th>
                  </tr>
                </thead>
                <tbody>
                  {filteredInventory.map((item, idx) => (
                    <tr
                      key={idx}
                      className="border-b border-strong hover:bg-elevated transition-colors"
                    >
                      <td className="px-4 py-3 text-sm text-primary max-w-xs">
                        <div
                          className="truncate"
                          title={item.product_title || "Unknown"}
                        >
                          {item.product_title || "Unknown product"}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-sm text-muted">
                        {item.variant_title &&
                        item.variant_title !== "Default Title"
                          ? item.variant_title
                          : "—"}
                      </td>
                      <td className="px-4 py-3 text-sm text-primary">
                        {locations.find((l) => l.id === item.location_id)
                          ?.name || `Location ${item.location_id}`}
                      </td>
                      <td className="px-4 py-3">
                        <input
                          type="number"
                          defaultValue={item.available}
                          className="field-input w-24 text-sm"
                          onBlur={(e) =>
                            handleUpdateQuantity(
                              item.inventory_item_id,
                              item.location_id,
                              parseInt(e.target.value, 10),
                            )
                          }
                        />
                      </td>
                      <td className="px-4 py-3 text-center">
                        <button
                          onClick={() => {
                            const newQty = prompt(
                              `New quantity for "${item.product_title || "item"}":`,
                              item.available,
                            );
                            if (
                              newQty !== null &&
                              !isNaN(parseInt(newQty, 10))
                            ) {
                              handleUpdateQuantity(
                                item.inventory_item_id,
                                item.location_id,
                                parseInt(newQty, 10),
                              );
                            }
                          }}
                          className="btn btn-secondary btn-sm"
                        >
                          <Ico n="edit" size="xs" /> Update
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
