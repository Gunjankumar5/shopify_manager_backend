-- ================================================================
-- FORCE REMOVE ALL RLS - Nuclear option to get system working
-- ================================================================
-- Drop ALL policies and disable RLS completely

-- Drop all policies on users table
DROP POLICY IF EXISTS "Users can view own profile" ON public.users;
DROP POLICY IF EXISTS "users_self_update" ON public.users;
DROP POLICY IF EXISTS "users_view_own" ON public.users;
DROP POLICY IF EXISTS "users_update_own" ON public.users;
DROP POLICY IF EXISTS "users_admin_view_all" ON public.users;
DROP POLICY IF EXISTS "users_admin_insert" ON public.users;
DROP POLICY IF EXISTS "users_admin_update" ON public.users;
DROP POLICY IF EXISTS "users_admin_delete" ON public.users;

-- Drop all policies on user_permissions table
DROP POLICY IF EXISTS "perms_view_own" ON public.user_permissions;
DROP POLICY IF EXISTS "perms_admin_all" ON public.user_permissions;

-- Disable RLS on both tables
ALTER TABLE public.users DISABLE ROW LEVEL SECURITY;
ALTER TABLE public.user_permissions DISABLE ROW LEVEL SECURITY;

-- Verify
SELECT 'RLS disabled for:' as status;
SELECT relname, relrowsecurity FROM pg_class 
WHERE relname IN ('users', 'user_permissions') AND relkind = 'r';

SELECT 'Remaining policies:' as status;
SELECT schemaname, tablename, policyname FROM pg_policies 
WHERE schemaname = 'public' AND tablename IN ('users', 'user_permissions');

SELECT 'Test query - all users:' as status;
SELECT id, email, role FROM public.users LIMIT 5;
