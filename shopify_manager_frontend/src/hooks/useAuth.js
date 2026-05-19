import { useState, useEffect, useRef } from "react";
import { supabase } from "../lib/supabaseClient";

const API_ORIGIN = (import.meta.env.VITE_API_ORIGIN || "http://127.0.0.1:8000").replace(/\/$/, "");
const API_BASE_URL = `${API_ORIGIN}/api`;
const PERMISSION_SELECT =
  "manage_products, delete_products, manage_collections, manage_inventory, " +
  "manage_metafields, manage_upload, manage_export, use_ai, manage_stores, " +
  "manage_users, view_analytics";

/**
 * useAuth hook - Centralized authentication state
 * Fetches user info, role, and permissions from Supabase
 * Returns: { user, role, permissions, loading, signOut, can }
 * - can(permission) - Check if user has a specific permission
 */
export function useAuth() {
  const [user, setUser] = useState(null);
  const [role, setRole] = useState(null);
  const [permissions, setPermissions] = useState({});
  const [loading, setLoading] = useState(true);
  const fetchInProgressRef = useRef(false);
  const lastUserIdRef = useRef(null);
  const onboardingAttemptedRef = useRef(new Set());

  useEffect(() => {
    // Get current session on mount
    supabase.auth.getSession().then(({ data: { session } }) => {
      setUser(session?.user || null);
      if (session?.user) {
        fetchUserDataOnce(session.user);
      } else {
        setLoading(false);
      }
    });

    // Listen for auth state changes
    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      async (event, session) => {
        setUser(session?.user || null);
        if (session?.user) {
          fetchUserDataOnce(session.user);
        } else {
          setRole(null);
          setPermissions({});
          setLoading(false);
          lastUserIdRef.current = null;
        }
      }
    );

    return () => subscription?.unsubscribe();
  }, []);

  async function maybeApplySignupRole(authUser, userData) {
    const desiredRole = String(authUser?.user_metadata?.desired_role || "")
      .trim()
      .toLowerCase();
    const adminEmail = String(authUser?.user_metadata?.admin_email || "").trim();

    if (!desiredRole || !["admin", "manager", "junior"].includes(desiredRole)) {
      return false;
    }

    const needsApply =
      userData?.role !== desiredRole ||
      ((desiredRole === "manager" || desiredRole === "junior") && !userData?.created_by);

    if (!needsApply) {
      return false;
    }

    if (onboardingAttemptedRef.current.has(authUser.id)) {
      return false;
    }
    onboardingAttemptedRef.current.add(authUser.id);

    const { data: sessionData } = await supabase.auth.getSession();
    const token = sessionData?.session?.access_token;
    if (!token) {
      return false;
    }

    const res = await fetch(`${API_BASE_URL}/users/me/role`, {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        role: desiredRole,
        admin_email: adminEmail || null,
      }),
    });

    if (!res.ok) {
      const err = await res.json().catch(() => ({}));
      throw new Error(err.detail || err.message || "Failed to apply signup role");
    }
    return true;
  }

  async function fetchUserDataOnce(authUser) {
    const userId = authUser?.id;
    if (!userId) return;

    // Don't fetch if already fetching or if we already fetched this user
    if (fetchInProgressRef.current || lastUserIdRef.current === userId) {
      return;
    }

    fetchInProgressRef.current = true;
    lastUserIdRef.current = userId;

    try {
      setLoading(true);

      // Fetch user with role
      let { data: userData, error: userError } = await supabase
        .from("users")
        .select("id, email, full_name, role, is_active, avatar_url, created_by")
        .eq("id", userId)
        .single();

      if (userError) {
        console.error("User fetch error:", userError);
        // Set defaults and stop trying to fetch
        setRole(null);
        setPermissions({});
        setLoading(false);
        return;
      }

      if (!userData) {
        console.error("No user data returned");
        setRole(null);
        setPermissions({});
        setLoading(false);
        return;
      }

      const onboardingApplied = await maybeApplySignupRole(authUser, userData);
      if (onboardingApplied) {
        const refreshed = await supabase
          .from("users")
          .select("id, email, full_name, role, is_active, avatar_url, created_by")
          .eq("id", userId)
          .single();
        userData = refreshed.data;
        userError = refreshed.error;
      }

      // Fetch user permissions
      let permData = {};
      try {
        const { data, error: permError } = await supabase
          .from("user_permissions")
          .select(PERMISSION_SELECT)
          .eq("user_id", userId)
          .single();

        if (!permError && data) {
          permData = data;
        } else if (permError && permError.code !== "PGRST116") {
          // PGRST116 = no rows found (expected for new users)
          console.warn("Permissions fetch warning:", permError);
        }

        // If user is admin and no permissions yet, grant all
        if (userData.role === "admin" && Object.keys(permData).length === 0) {
          permData = {
            manage_products: true,
            delete_products: true,
            manage_collections: true,
            manage_inventory: true,
            manage_metafields: true,
            manage_upload: true,
            manage_export: true,
            use_ai: true,
            manage_stores: true,
            manage_users: true,
            view_analytics: true,
          };
        }
      } catch (err) {
        console.warn("Permissions error:", err);
        // For admins, grant all permissions anyway
        if (userData.role === "admin") {
          permData = {
            manage_products: true,
            delete_products: true,
            manage_collections: true,
            manage_inventory: true,
            manage_metafields: true,
            manage_upload: true,
            manage_export: true,
            use_ai: true,
            manage_stores: true,
            manage_users: true,
            view_analytics: true,
          };
        }
      }

      setPermissions(permData || {});
      setRole(userData.role || null);
    } catch (err) {
      console.error("Auth fetch error:", err);
      setRole(null);
      setPermissions({});
    } finally {
      setLoading(false);
      fetchInProgressRef.current = false;
    }
  }

  async function signOut() {
    await supabase.auth.signOut();
    setUser(null);
    setRole(null);
    setPermissions({});
    lastUserIdRef.current = null;
    fetchInProgressRef.current = false;
  }

  function can(permission) {
    // Admin has all permissions
    if (role === "admin") return true;
    // Check specific permission
    return permissions[permission] === true;
  }

  return {
    user,
    role,
    permissions,
    loading,
    signOut,
    can,
  };
}
