-- ================================================================
-- COMPLETE USER MANAGEMENT MIGRATION
-- ================================================================
-- 
-- NEW FLOW:
-- 1. User signs up/logs in via LoginPage (Supabase Auth)
-- 2. Automatically creates user in public.users table
-- 3. User clicks "User Management"
-- 4. Selects role (Admin/Manager/Junior)
-- 5. Role + permissions saved to localStorage
-- 6. Based on role, features appear/disappear in sidebar
-- 7. "Connect Store" checks permissions
--
-- This migration works with your existing schema:
-- - users (id, email, created_at, updated_at)
-- - connected_stores
-- - user_stores
--
-- ================================================================
-- STEP 1: Extend existing `users` table
-- ================================================================
-- Adds: full_name, role, is_active, created_by, last_login, avatar_url
-- Uses IF NOT EXISTS so safe to re-run

ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS full_name    TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS role         TEXT NOT NULL DEFAULT 'junior'
                                        CHECK (role IN ('admin', 'manager', 'junior')),
  ADD COLUMN IF NOT EXISTS is_active    BOOLEAN NOT NULL DEFAULT TRUE,
  ADD COLUMN IF NOT EXISTS created_by   UUID REFERENCES public.users(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS last_login   TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS avatar_url   TEXT;


-- ================================================================
-- STEP 2: Create user_permissions table
-- ================================================================
-- Stores 11 granular permissions for each user
-- Separate from users table for flexibility

DROP TABLE IF EXISTS public.user_permissions CASCADE;

CREATE TABLE IF NOT EXISTS public.user_permissions (
  user_id              UUID PRIMARY KEY REFERENCES public.users(id) ON DELETE CASCADE,
  
  -- Products management
  manage_products      BOOLEAN NOT NULL DEFAULT FALSE,
  delete_products      BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Collections
  manage_collections   BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Inventory
  manage_inventory     BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Metafields
  manage_metafields    BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Upload & Export
  manage_upload        BOOLEAN NOT NULL DEFAULT FALSE,
  manage_export        BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- AI features
  use_ai               BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Store management (required for "Connect Store")
  manage_stores        BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- User management (admin only)
  manage_users         BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Analytics
  view_analytics       BOOLEAN NOT NULL DEFAULT FALSE,
  
  -- Metadata
  created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);


-- ================================================================
-- STEP 3: Create trigger for auto-creating users on signup
-- ================================================================
-- When someone signs up via Supabase Auth, automatically:
-- 1. Insert into public.users
-- 2. Insert into public.user_permissions (defaults all to FALSE)

CREATE OR REPLACE FUNCTION public.handle_new_auth_user()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  -- Create user record if not exists
  IF NOT EXISTS (SELECT 1 FROM public.users WHERE id = NEW.id) THEN
    INSERT INTO public.users (
      id, 
      email, 
      full_name, 
      role, 
      is_active, 
      created_at, 
      updated_at
    )
    VALUES (
      NEW.id,
      NEW.email,
      COALESCE(NEW.raw_user_meta_data->>'full_name', split_part(NEW.email, '@', 1)),
      'junior',  -- Default role on signup
      TRUE,
      NOW(),
      NOW()
    );
  END IF;

  -- Create permissions record with all defaults to FALSE
  IF NOT EXISTS (SELECT 1 FROM public.user_permissions WHERE user_id = NEW.id) THEN
    INSERT INTO public.user_permissions (user_id, created_at, updated_at)
    VALUES (NEW.id, NOW(), NOW());
  END IF;

  RETURN NEW;
END;
$$;

-- Drop old trigger if exists, recreate
DROP TRIGGER IF EXISTS on_auth_user_created ON auth.users;
CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.handle_new_auth_user();


-- ================================================================
-- STEP 4: Create trigger for tracking last_login
-- ================================================================
-- When user logs in, update last_login timestamp

CREATE OR REPLACE FUNCTION public.handle_auth_user_login()
RETURNS TRIGGER LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
  IF NEW.last_sign_in_at IS DISTINCT FROM OLD.last_sign_in_at THEN
    UPDATE public.users
    SET last_login = NOW(), updated_at = NOW()
    WHERE id = NEW.id;
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS on_auth_user_login ON auth.users;
CREATE TRIGGER on_auth_user_login
  AFTER UPDATE ON auth.users
  FOR EACH ROW EXECUTE FUNCTION public.handle_auth_user_login();


-- ================================================================
-- STEP 5: Backfill permissions for existing users
-- ================================================================
-- Ensures every user in public.users has a permissions row

INSERT INTO public.user_permissions (user_id, created_at, updated_at)
SELECT id, NOW(), NOW() FROM public.users
WHERE id NOT IN (SELECT user_id FROM public.user_permissions)
ON CONFLICT (user_id) DO NOTHING;


-- ================================================================
-- STEP 6: Enable Row Level Security (RLS)
-- ================================================================
-- Restricts data access based on user role

ALTER TABLE public.users ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_permissions ENABLE ROW LEVEL SECURITY;


-- ================================================================
-- STEP 7: Drop old RLS policies (safe re-run)
-- ================================================================

DROP POLICY IF EXISTS "users_view_own"       ON public.users;
DROP POLICY IF EXISTS "users_admin_view_all" ON public.users;
DROP POLICY IF EXISTS "users_admin_insert"   ON public.users;
DROP POLICY IF EXISTS "users_admin_update"   ON public.users;
DROP POLICY IF EXISTS "users_admin_delete"   ON public.users;
DROP POLICY IF EXISTS "perms_view_own"       ON public.user_permissions;
DROP POLICY IF EXISTS "perms_admin_all"      ON public.user_permissions;


-- ================================================================
-- STEP 8: Create RLS policies for `users` table
-- ================================================================

-- Policy: Users can view their own record
CREATE POLICY "users_view_own"
  ON public.users FOR SELECT
  USING (id = auth.uid());

-- Policy: Admins can view all users
CREATE POLICY "users_admin_view_all"
  ON public.users FOR SELECT
  USING (
    EXISTS (SELECT 1 FROM public.users WHERE id = auth.uid() AND role = 'admin')
  );

-- Policy: Admin/Manager can create new users
CREATE POLICY "users_admin_insert"
  ON public.users FOR INSERT
  WITH CHECK (
    EXISTS (SELECT 1 FROM public.users WHERE id = auth.uid() AND role IN ('admin', 'manager'))
  );

-- Policy: Only admins can update users
CREATE POLICY "users_admin_update"
  ON public.users FOR UPDATE
  USING (
    EXISTS (SELECT 1 FROM public.users WHERE id = auth.uid() AND role = 'admin')
  );

-- Policy: Only admins can delete users
CREATE POLICY "users_admin_delete"
  ON public.users FOR DELETE
  USING (
    EXISTS (SELECT 1 FROM public.users WHERE id = auth.uid() AND role = 'admin')
  );


-- ================================================================
-- STEP 9: Create RLS policies for `user_permissions` table
-- ================================================================

-- Policy: Users can view their own permissions
CREATE POLICY "perms_view_own"
  ON public.user_permissions FOR SELECT
  USING (user_id = auth.uid());

-- Policy: Admins have full access to all permissions
CREATE POLICY "perms_admin_all"
  ON public.user_permissions FOR ALL
  USING (
    EXISTS (SELECT 1 FROM public.users WHERE id = auth.uid() AND role = 'admin')
  );


-- ================================================================
-- STEP 10: Create performance indexes
-- ================================================================
-- Speed up common queries

CREATE INDEX IF NOT EXISTS idx_user_permissions_user_id 
  ON public.user_permissions(user_id);

CREATE INDEX IF NOT EXISTS idx_users_email 
  ON public.users(email);

CREATE INDEX IF NOT EXISTS idx_users_role 
  ON public.users(role);

CREATE INDEX IF NOT EXISTS idx_users_is_active 
  ON public.users(is_active);


-- ================================================================
-- STEP 11: PROMOTE YOUR ACCOUNT TO ADMIN
-- ================================================================
-- 
-- UPDATE the following with YOUR email, then uncomment and run both commands
--
-- ⚠️ IMPORTANT: Replace 'ss222802@gmail.com' with your actual email!

UPDATE public.users
SET role = 'admin', full_name = 'Admin User', updated_at = NOW()
WHERE email = 'ss222802@gmail.com';

UPDATE public.user_permissions
SET
  manage_products    = TRUE,
  delete_products    = TRUE,
  manage_collections = TRUE,
  manage_inventory   = TRUE,
  manage_metafields  = TRUE,
  manage_upload      = TRUE,
  manage_export      = TRUE,
  use_ai             = TRUE,
  manage_stores      = TRUE,
  manage_users       = TRUE,
  view_analytics     = TRUE,
  updated_at         = NOW()
WHERE user_id = (SELECT id FROM public.users WHERE email = 'ss222802@gmail.com');


-- ================================================================
-- STEP 12: VERIFY EVERYTHING WORKED
-- ================================================================
-- Run this to confirm your admin account is set up correctly

SELECT 
  u.id, 
  u.email, 
  u.role, 
  u.full_name,
  u.is_active,
  u.created_at,
  u.last_login,
  p.manage_products,
  p.manage_collections,
  p.manage_inventory,
  p.manage_metafields,
  p.manage_upload,
  p.manage_export,
  p.use_ai,
  p.manage_stores,
  p.manage_users,
  p.view_analytics
FROM public.users u
LEFT JOIN public.user_permissions p ON p.user_id = u.id
WHERE u.email = 'ss222802@gmail.com';


-- ================================================================
-- STEP 13: VIEW ALL USERS & PERMISSIONS
-- ================================================================
-- See all users and their roles/permissions

SELECT 
  u.id,
  u.email,
  u.role,
  u.full_name,
  u.is_active,
  u.created_at,
  p.manage_products,
  p.manage_stores,
  p.manage_users
FROM public.users u
LEFT JOIN public.user_permissions p ON p.user_id = u.id
ORDER BY u.created_at DESC;
