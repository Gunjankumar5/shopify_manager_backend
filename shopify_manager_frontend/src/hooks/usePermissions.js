import { useEffect, useState, useCallback } from "react";
import { api } from "../api/api";
import { supabase } from "../lib/supabaseClient";

/**
 * usePermissions Hook - DEPRECATED: Use useAuth instead
 * Kept for backward compatibility
 */
export function usePermissions() {
  const [permissions, setPermissions] = useState({});
  const [role, setRole] = useState("junior");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // Fetch permissions on mount
  useEffect(() => {
    const fetchPermissions = async () => {
      try {
        setLoading(true);
        setError(null);

        // Get current user info with permissions
        const response = await api.get(`/users/me`, {
          ttlMs: 300000,
          persist: true,
          force: true,
        });

        if (response && typeof response === "object") {
          const userPerms = response.permissions || {};
          const userRole = response.role || "junior";
          
          setPermissions(userPerms);
          setRole(userRole);
        }
      } catch (err) {
        console.warn("usePermissions error:", err);
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };

    fetchPermissions();
  }, []);

  const can = useCallback((permission) => {
    if (role === "admin") return true;
    return permissions[permission] === true;
  }, [role, permissions]);

  return {
    permissions,
    role,
    loading,
    error,
    can,
  };
}

